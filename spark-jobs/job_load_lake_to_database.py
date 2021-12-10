import base64

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from utils import arg_utils, database_utils, s3_utils

job_args = arg_utils.get_job_args()

job_name = f"job_load_{job_args.source_schema}_{job_args.source_table}"
jobExec = jobControl.Job(job_name, job_args)

target_connection_properties = {
    "database": job_args.database,
    "hostname": job_args.database_host,
    "port": job_args.database_port,
    "username": job_args.database_user,
    "password": job_args.database_password,
}

jdbc_dict_params = {
    "user": job_args.database_user,
    "password": job_args.database_password,
    "driver": database_utils.spark_properties_database_driver[job_args.database_driver],
}


def main():
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
        except Exception as e:
            exception_msg = str(e)
            print(
                "Something went wrong:",
                exception_msg.split("\n")[1].split("Exception: ")[1],
            )
            raise e

        df = spark.sql(str(base64.b64decode(job_args.spark_sql_query), "UTF-8"))
        sql_columns = df.columns

        # missing_columns = list(set(source_columns).difference(set(sql_columns)))

        # if missing_columns:
        #     for missing_column in missing_columns:
        #         df = df.withColumn(missing_column, f.lit(None).cast(dtype))

        df = df.select(*source_columns)

        try:
            df.cache()
            jobExec.totalLines = df.count()
            assert jobExec.totalLines > 0
        except Exception as e:
            print("Spark query returned zero lines dataframe, aborting job")
            raise e

        try:
            df.write.option("truncate", "true").option(
                "numPartitions", jobExec.calcNumPartitions()
            ).jdbc(
                url=f"jdbc:{job_args.database_driver.lower()}://{job_args.database_host}:{job_args.database_port}/{job_args.database}",
                table=f"{jobExec.source_schema}.{jobExec.source_table}",
                mode="overwrite",
                properties=jdbc_dict_params,
            )
        except Exception as e:
            print("Failed to insert lines in database target table")
            raise e

        try:
            jobExec.totalLines = db_conn.run(
                {
                    "command_type": "select",
                    "schema": f"{jobExec.source_schema}",
                    "columns": "count(1)",
                    "table": f"{jobExec.source_table}",
                    "where_clause": "1=1",
                }
            )[0][0]
        except Exception as e:
            print("Unable to verify number of inserted lines in Database target table")
            raise e

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
        add_hive_path=False,
        delete_excessive_files=False,
        infer_partitions=False,
    )
