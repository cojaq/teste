import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType
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

    df_page_view = (
        spark.table(f"{jobExec.database_web_analytics}.page_view")
        .filter(f.col("reference_date") == int(jobExec.reference_date))
        .filter(f.col("app_id") == f.lit("GymPass"))
    )

    df_stg_person_type = spark.table(f"{jobExec.database_work}.stg_person_type")

    df_dim_products = (
        df_page_view.withColumnRenamed("event_id", "page_view_id")
        .join(df_stg_person_type, "person_id", "left")
        .withColumn(
            "created_at_converted",
            f.when(
                f.col("os_timezone").isNotNull(),
                f.expr("from_utc_timestamp(derived_tstamp, os_timezone)"),
            )
            .when(
                f.col("geo_timezone").isNotNull(),
                f.expr("from_utc_timestamp(derived_tstamp, os_timezone)"),
            )
            .otherwise(f.col("derived_tstamp")),
        )
        .withColumn(
            "date",
            (f.date_format("created_at_converted", "yyyyMMdd")).cast(IntegerType()),
        )
        .withColumn(
            "hour",
            (f.date_format("created_at_converted", "H")).cast(IntegerType()),
        )
        .withColumn(
            "minute",
            (f.date_format("created_at_converted", "m")).cast(IntegerType()),
        )
        .withColumn(
            "person_type",
            f.when(f.upper(f.col("useragent")).like("%BOT%"), f.lit("BOT"))
            .when(
                df_stg_person_type["person_type"].isNotNull(),
                df_stg_person_type["person_type"],
            )
            .otherwise(f.lit("REGULAR USER")),
        )
        .withColumn("utc_date", f.col("reference_date"))
        .select(
            f.col("page_view_id"),
            "date",
            "hour",
            "minute",
            f.col("geo_country").alias("page_view_country_name"),
            "viewer_id",
            "person_id",
            "company_id",
            "person_type",
            f.col("mkt_source").alias("utm_source"),
            f.col("mkt_medium").alias("utm_medium"),
            f.col("mkt_campaign").alias("utm_campaign"),
            f.col("mkt_term").alias("utm_term"),
            f.col("mkt_content").alias("utm_content"),
            "latitude",
            "longitude",
            "utc_date",
            "reference_date",
        )
    )

    df_dim_products = dataframe_utils.createPartitionColumns(
        df_dim_products, jobExec.reference_date
    )

    df_dim_products = jobExec.select_dataframe_columns(
        spark, df_dim_products, table_columns
    )
    df_dim_products = df_dim_products.repartition(num_partitions, "page_view_id")

    df_dim_products.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        (spark.table(f"{jobExec.target_schema}.{jobExec.target_table}"))
        .filter(f.col("reference_date") == jobExec.reference_date)
        .count()
    )

    if jobExec.totalLines > 0:
        table_location = dataframe_utils.returnHiveTableLocation(
            spark,
            jobExec.target_schema,
            jobExec.target_table,
            True,
            jobExec.reference_date,
        )

        delete_statement = f"DELETE FROM {jobExec.database_public}.{jobExec.target_table} WHERE utc_date = {jobExec.reference_date}"
        jobExec.redshift.executeStatement(delete_statement, "delete")

        jobExec.redshift.LoadS3toRedshift(
            table_location, jobExec.database_public, jobExec.target_table
        )
    else:
        jobExec.logger.warning("Target table is empty")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
