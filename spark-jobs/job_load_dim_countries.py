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

    # Reading source table from ODS MariaDB
    df_countries = (
        spark.table(f"{jobExec.database_replica_full}.countries")
        .select(
            "id",
            "title",
            "timezone",
            "short_title",
            "enabled",
            "default_currency_id",
        )
        .filter(f.col("enabled") == 1)
    )

    df_currencies = spark.table(f"{jobExec.database_replica_full}.currencies").select(
        f.col("id").alias("default_currency_id"),
        f.col("short_title").alias("currency_id"),
    )

    df_country_informations = spark.table(
        f"{jobExec.database_ods}.country_informations"
    ).select(
        "short_title",
        "country_region_code",
        "country_region",
        "country_region_group",
    )

    df_dim_countries = (
        df_countries.join(df_country_informations, "short_title", "left")
        .join(df_currencies, "default_currency_id", "left")
        .select(
            "id",
            "country_region_group",
            f.col("title").alias("country_title"),
            "timezone",
            "currency_id",
            "country_region",
            "short_title",
        )
    )

    df_dim_countries = jobExec.select_dataframe_columns(
        spark, df_dim_countries, table_columns
    )
    df_dim_countries = df_dim_countries.repartition(num_partitions, "id")

    df_dim_countries.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
