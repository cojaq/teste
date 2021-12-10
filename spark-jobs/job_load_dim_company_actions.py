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
    jobExec.target_schema if jobExec.target_schema else jobExec.database_edw
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    list_actions = []
    actions = jobExec.selectFilterActions("COMPANY")

    for i in actions:
        list_actions.append(i[0])

    df_stg_versions = spark.table(f"{jobExec.database_work}.stg_versions").filter(
        (f.col("reference_date") == jobExec.reference_date)
        & (f.col("item_type") == "Company")
        & (f.upper(f.col("action")).isin(list_actions))
    )

    df_dim_company_actions = df_stg_versions.select(
        f.col("item_id").alias("company_id"),
        f.col("created_at").alias("action_date"),
        "action",
        "old_value",
        "new_value",
    )

    df_dim_company_actions = dataframe_utils.createPartitionColumns(
        df_dim_company_actions, jobExec.reference_date
    )

    df_dim_company_actions = jobExec.select_dataframe_columns(
        spark, df_dim_company_actions, table_columns
    )
    df_dim_company_actions = df_dim_company_actions.repartition(
        num_partitions, "company_id"
    )

    df_dim_company_actions.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        (spark.table(f"{jobExec.target_schema}.{jobExec.target_table}"))
        .filter(f.col("reference_date") == jobExec.reference_date)
        .count()
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
