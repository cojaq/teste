import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema
    if jobExec.target_schema
    else jobExec.database_replica_full_history
)


def get_next_max_id(df):
    return str(df.select(f.max("id")).collect()[0][0])


def main():
    # This list is necessary to treat tables that ara extract incrementally

    query = jobExec.dict_job["query"]
    partition_column = jobExec.dict_job["partition_column"]

    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    jobExec.logger.info(f"Query: {query}")

    # Calc the number of partitions and the lower and upper bounds
    upper_bound, lower_bound, count_lines = jobExec.returnBound()

    jobExec.logger.info(
        f"upper_bound: {upper_bound}, lower_bound: {lower_bound}, count_lines: {count_lines}"
    )

    jobExec.totalLines = count_lines
    num_partitions = jobExec.calcNumPartitions()

    if jobExec.totalLines == 0:
        jobExec.logger.warning("Skipping extract job because query return zero results")
    else:
        # Source and Target properties
        dict_source_table = dict(
            host=jobExec.dict_job["host"],
            options=dict(
                dbtable=query,
                partitionColumn=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
            ),
        )

        # Reading source table from Replica Full
        df_source = jobExec.sparkReadJdbc(spark, dict_source_table)

        if jobExec.dict_job["partitionBy"]:
            df_source = dataframe_utils.createPartitionColumns(
                df_source, jobExec.reference_date
            )

        if jobExec.dict_job["write_mode"] == "overwrite":
            flg_overwrite = True
        else:
            flg_overwrite = False

        df_source = jobExec.select_dataframe_columns(spark, df_source, table_columns)
        df_source = df_source.repartition(num_partitions, partition_column)
        df_source.write.insertInto(
            f"{jobExec.target_schema}.{jobExec.target_table}",
            overwrite=flg_overwrite,
        )

        if jobExec.dict_job["extract_mode"] == "maxid":
            df_max_id = spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
            jobExec.maxID = get_next_max_id(df_max_id)


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
