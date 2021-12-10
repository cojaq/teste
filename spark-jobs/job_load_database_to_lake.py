from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, database_utils, s3_utils

job_args = arg_utils.get_job_args()

job_name = f"job_load_{job_args.target_schema}_{job_args.table}"
jobExec = jobControl.Job(job_name, job_args)

target_connection_properties = {
    "database": job_args.database,
    "hostname": job_args.database_host,
    "port": job_args.database_port,
    "username": job_args.database_user,
    "password": job_args.database_password,
}

jdbc_dict_params = {
    "url": f"jdbc:{job_args.database_driver.lower()}://{job_args.database_host}:{job_args.database_port}/{job_args.database}",
    "user": job_args.database_user,
    "password": job_args.database_password,
    "driver": database_utils.spark_properties_database_driver[job_args.database_driver],
}

skip_char_dict = {"postgresql": '"', "postgres": '"', "mysql": "`"}


def main():
    try:
        hive_columns = spark.table(
            f"{jobExec.target_schema}.{jobExec.target_table}"
        ).columns
        assert (len(hive_columns)) > 0
    except AssertionError as hive_columns_error:
        print(
            f"Error - Hive returned no columns for {jobExec.target_schema}.{jobExec.target_table}"
        )
        raise hive_columns_error
    except Exception as e:
        print("Something else went wrong:", e)
        raise e

    with getattr(database_utils, job_args.database_driver)(
        target_connection_properties
    ) as db_conn:
        try:
            source_columns = db_conn.run(
                {
                    "command_type": "select",
                    "schema": "information_schema",
                    "columns": "COLUMN_NAME",
                    "table": "columns",
                    "where_clause": f"table_schema='{jobExec.source_schema}' AND table_name='{jobExec.source_table}'",
                }
            )
            source_columns = [source_column[0] for source_column in source_columns]
            assert (len(source_columns)) > 0
        except AssertionError as source_columns_error:
            print(
                f"Error - No columns returned for {jobExec.source_schema}.{jobExec.source_table}"
            )
            raise source_columns_error
        except Exception as e:
            print("Something else went wrong:", e)
            raise e

        common_columns = list(set(hive_columns).intersection(set(source_columns)))

        try:
            table_count = db_conn.run(
                {
                    "command_type": "select",
                    "schema": f"{jobExec.source_schema}",
                    "columns": "count(1)",
                    "table": f"{jobExec.source_table}",
                    "where_clause": "1=1",
                }
            )
        except Exception as e:
            print(
                f"Failed counting lines for {jobExec.source_schema}.{jobExec.source_table}"
            )
            print("Error message:", e)
            raise e

        jobExec.totalLines = [total_lines[0] for total_lines in table_count][0]
        jdbc_dict_params["numPartitions"] = jobExec.calcNumPartitions()
        sel_columns = ", ".join(
            skip_char_dict.get(job_args.database_driver.lower(), "")
            + column
            + skip_char_dict.get(job_args.database_driver.lower(), "")
            for column in common_columns
        )
        jdbc_dict_params[
            "query"
        ] = f"""SELECT {sel_columns} FROM {jobExec.source_schema}.{jobExec.source_table}"""

        try:
            df = spark.read.format("jdbc").options(**jdbc_dict_params).load()
        except Exception as read_error:
            print(
                f"Error - failed to read data from {jobExec.source_schema}.{jobExec.source_table}"
            )
            raise read_error

        missing_columns = list(set(hive_columns).difference(set(source_columns)))

        if missing_columns:
            for missing_column in missing_columns:
                df = df.withColumn(missing_column, f.lit(None))

        df = df.select(*hive_columns)
        df = df.repartition(6)
        df.write.insertInto(
            f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
        )

        jobExec.totalLines = df.count()

        if spark._jsparkSession.catalog().tableExists(
            f"{jobExec.target_schema}_history.{jobExec.target_table}"
        ):
            try:
                df_history = df.withColumn(
                    "reference_date", f.lit(int(jobExec.reference_date))
                )
                df_history = df_history.repartition(6, "reference_date")
                df_history.write.insertInto(
                    f"{jobExec.target_schema}_history.{jobExec.target_table}",
                    overwrite=True,
                )
                print(
                    f"Success - inserted to History on {jobExec.target_schema}_history.{jobExec.target_table}"
                )
            except Exception as err:
                print(
                    f"Error - failed to insert History on {jobExec.target_schema}_history.{jobExec.target_table}"
                )
                print(f"Error: - {err}")
                pass

    s3_utils.put_json(
        jobExec.config_path,
        f"etl-jobs/job_timestamps/{job_name}/{jobExec.reference_time}.json",
        {"reference_time": jobExec.reference_time},
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=False,
        infer_partitions=False,
    )
