import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 4

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema if jobExec.target_schema else jobExec.database_salesforce
)
history_salesforce_path = (
    f"salesforce/by_date/{jobExec.reference_date}/{jobExec.target_table}"
)


def load_salesforce_last_reference_table():
    try:
        df_salesforce_last_table = spark.read.parquet(
            f"s3://{jobExec.structured_bucket}/salesforce/by_date/{jobExec.last_reference_date}/{jobExec.target_table}"
        )
        run_upsert = True

    except Exception as exception:
        jobExec.logger.warning(
            f"Path to last reference date salesforce {jobExec.target_table} doesn't exist. Upsert step will be skipped"
        )
        df_salesforce_last_table = None
        run_upsert = False

    return df_salesforce_last_table, run_upsert


def salesforce_table_update(
    df_salesforce_table, df_salesforce_table_update, run_upsert
):
    if run_upsert:
        df_salesforce_table_unchanged = df_salesforce_table.join(
            df_salesforce_table_update, "id", "left_anti"
        )

        df_salesforce_table_update = df_salesforce_table_unchanged.unionByName(
            df_salesforce_table_update
        )

    return df_salesforce_table_update


def main():
    salesforce_table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    (
        df_salesforce_last_table,
        run_upsert,
    ) = load_salesforce_last_reference_table()
    if df_salesforce_last_table:
        df_salesforce_last_table = dataframe_utils.normalize_dataframe_columns(
            df_salesforce_last_table, salesforce_table_columns
        )

    df_salesforce_table_upsert = (
        spark.table(f"{jobExec.database_salesforce_raw}.{jobExec.target_table}")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .drop("reference_date")
        .filter(f.lower(f.col("id")) != "id")
        .replace("null", None)
        .replace("", None)
    )
    df_salesforce_table_upsert.cache()

    salesforce_table_updated = salesforce_table_update(
        df_salesforce_last_table, df_salesforce_table_upsert, run_upsert
    ).select(*salesforce_table_columns)

    salesforce_table_updated.repartition(num_partitions).write.parquet(
        f"s3://{jobExec.structured_bucket}/{history_salesforce_path}",
        mode="overwrite",
    )

    salesforce_table_updated.repartition(num_partitions).write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = df_salesforce_table_upsert.count()

    if jobExec.totalLines == 0:
        jobExec.logger.warning(
            f"No line to be upserted in table {jobExec.database_salesforce_raw}.{jobExec.target_table}"
        )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
    jobExec.delete_excessive_files(
        spark,
        target_bucket=jobExec.structured_bucket,
        target_path=f"{history_salesforce_path}/",
    )
