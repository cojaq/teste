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
    jobExec.target_schema if jobExec.target_schema else jobExec.database_work
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    # Reading source table from ODS MariaDB
    df_viewers = spark.table(f"{jobExec.database_replica_full}.viewers").filter(
        f.col("person_id").isNotNull()
    )

    df_company_members = spark.table(
        f"{jobExec.database_replica_full}.company_members"
    ).select("person_id", "company_id", "created_at", "disabled_at")

    df_stg_viewer_company_member = (
        df_viewers.join(
            df_company_members,
            [
                df_viewers["person_id"] == df_company_members["person_id"],
                df_viewers["created_at"] >= df_company_members["created_at"],
                df_viewers["created_at"]
                <= f.coalesce(df_company_members["disabled_at"], f.lit("2100-12-31")),
            ],
            "inner",
        )
        .groupBy(
            f.col("id").alias("viewer_id"),
            df_viewers["person_id"],
            "updated_at",
        )
        .agg(f.max("company_id").alias("company_id"))
    )

    df_stg_viewer_company_member = jobExec.select_dataframe_columns(
        spark, df_stg_viewer_company_member, table_columns
    )
    df_stg_viewer_company_member = df_stg_viewer_company_member.repartition(
        num_partitions, "viewer_id"
    )

    df_stg_viewer_company_member.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
