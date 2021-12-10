import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 6

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema if jobExec.target_schema else jobExec.database_replica_full
)


def main():
    # This list is necessary to treat tables that ara extract incrementally
    partitioned_tables = ["page_views", "versions"]

    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    df_latest = spark.table(
        f"{jobExec.database_replica_full_history}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.reference_date)
    if jobExec.target_table not in partitioned_tables:
        df_latest = df_latest.drop("reference_date")

    df_latest = jobExec.select_dataframe_columns(spark, df_latest, table_columns)
    df_latest = df_latest.repartition(num_partitions)
    df_latest.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
