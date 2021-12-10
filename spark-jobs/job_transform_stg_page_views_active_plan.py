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
    df_page_views = (
        spark.table(f"{jobExec.database_replica_full_history}.page_views")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .select("id", "person_id", "created_at")
    )

    df_stg_carts_products = spark.table(
        f"{jobExec.target_schema}.stg_carts_products"
    ).select(
        "person_id",
        "valid_start_day",
        "valid_end_day",
        "report_id",
        "product_id",
    )

    df_stg_page_views_active_plan = (
        df_page_views.filter(f.col("person_id") != 0)
        .join(
            df_stg_carts_products.filter(
                (df_stg_carts_products["report_id"].isin(1, 17, 18, 20, 29))
                & (df_stg_carts_products["product_id"] != 19)
            ),
            [
                df_page_views["person_id"] == df_stg_carts_products["person_id"],
                df_page_views["created_at"] >= df_stg_carts_products["valid_start_day"],
                df_page_views["created_at"] <= df_stg_carts_products["valid_end_day"],
            ],
            "inner",
        )
        .select(
            df_page_views["id"].alias("page_view_id"),
            f.lit(True).alias("flag_active_plan_value"),
        )
    )

    df_stg_page_views_active_plan = jobExec.select_dataframe_columns(
        spark, df_stg_page_views_active_plan, table_columns
    )
    df_stg_page_views_active_plan = df_stg_page_views_active_plan.repartition(
        num_partitions, "page_view_id"
    )

    df_stg_page_views_active_plan = dataframe_utils.createPartitionColumns(
        df_stg_page_views_active_plan, jobExec.reference_date
    )

    df_stg_page_views_active_plan.write.insertInto(
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
