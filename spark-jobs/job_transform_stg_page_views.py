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


def filter_replica_page_views(df_page_views, reference_date, filter_date):
    if int(reference_date) < filter_date:
        new_df_page_views = (
            df_page_views.filter(f.col("reference_date") < filter_date)
            .filter(f.col("person_id").isNotNull())
            .filter(f.col("person_id") != 0)
        )
    else:
        new_df_page_views = df_page_views.filter(f.col("reference_date") == 0)

    return new_df_page_views


def create_reference_date_page_views(df_stg_page_views, reference_date, filter_date):
    ref_date_page_views_replica = 20181201

    if int(jobExec.reference_date) < filter_date:
        new_df_stg_page_views = dataframe_utils.createPartitionColumns(
            df_stg_page_views, ref_date_page_views_replica
        )
    else:
        new_df_stg_page_views = dataframe_utils.createPartitionColumns(
            df_stg_page_views, reference_date
        )

    return new_df_stg_page_views


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    first_snowplow_event_date = 20190101

    # Reading source table from ODS MariaDB
    df_page_views = spark.table(f"{jobExec.database_replica_full}.page_views").select(
        "person_id", "created_at", "country_id", "reference_date"
    )

    df_page_views = filter_replica_page_views(
        df_page_views, jobExec.reference_date, first_snowplow_event_date
    )

    df_countries = spark.table(f"{jobExec.database_replica_full}.countries").select(
        "id", "timezone"
    )

    df_snowplow_page_view = (
        spark.table(f"{jobExec.database_web_analytics}.page_view")
        .filter(f.col("user_id").isNotNull())
        .filter(f.col("reference_date") >= first_snowplow_event_date)
        .filter(f.col("reference_date") == jobExec.reference_date)
        .select(
            f.col("user_id").alias("person_id"),
            f.col("derived_tstamp").alias("created_at"),
            "os_timezone",
        )
        .distinct()
    )

    df_stg_page_views_replica = (
        df_page_views.join(
            f.broadcast(df_countries.withColumnRenamed("id", "country_id")),
            "country_id",
            "left",
        )
        .select(
            "person_id",
            "created_at",
            f.expr("from_utc_timestamp(created_at, timezone)").alias(
                "created_at_converted"
            ),
        )
        .distinct()
    )

    df_stg_page_views_snowplow = df_snowplow_page_view.select(
        "person_id",
        "created_at",
        f.expr("from_utc_timestamp(created_at, os_timezone)").alias(
            "created_at_converted"
        ),
    ).distinct()

    df_stg_page_views = df_stg_page_views_replica.union(df_stg_page_views_snowplow)

    df_stg_page_views = create_reference_date_page_views(
        df_stg_page_views, jobExec.reference_date, first_snowplow_event_date
    )

    df_stg_page_views = jobExec.select_dataframe_columns(
        spark, df_stg_page_views, table_columns
    )
    df_stg_page_views = df_stg_page_views.repartition(num_partitions, "person_id")

    df_stg_page_views.write.insertInto(
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
