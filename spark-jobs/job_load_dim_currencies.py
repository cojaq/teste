import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType, IntegerType
from utils import arg_utils, dataframe_utils, date_utils

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

    # Reading source table
    udf_quarter_last_date = f.udf(
        lambda x, y: date_utils.quarter_last_date(x, y), IntegerType()
    )

    df_currency_rates = (
        spark.table("{}.currency_rates".format(jobExec.database_replica_full))
        .select(
            "id",
            "to_currency_id",
            "from_currency_id",
            "considered_at",
            f.col("rate").cast(DecimalType(16, 8)),
        )
        .filter(f.col("from_currency_id") == 840)
    )

    df_currencies = spark.table(
        "{}.currencies".format(jobExec.database_replica_full)
    ).select("id", "short_title")

    df_countries = (
        spark.table("{}.countries".format(jobExec.database_replica_full))
        .select(
            "id",
            "title",
            f.upper(f.col("short_title").substr(1, 150)).alias("country_short_title"),
            "default_currency_id",
            "enabled",
        )
        .filter(f.col("enabled") == 1)
    )

    # Transform

    df_dim_currencies_quarter = (
        df_currency_rates.withColumn(
            "date_id",
            udf_quarter_last_date(
                df_currency_rates["considered_at"],
                f.quarter(df_currency_rates["considered_at"]),
            ),
        )
        .join(
            df_currencies,
            df_currencies["id"] == df_currency_rates["to_currency_id"],
            "inner",
        )
        .join(
            df_countries,
            df_countries["default_currency_id"] == df_currencies["id"],
            "inner",
        )
        .groupBy(
            "date_id",
            df_currency_rates["from_currency_id"],
            df_currency_rates["to_currency_id"],
            df_currencies["short_title"],
            df_countries["country_short_title"],
        )
        .agg(
            f.max(df_countries["id"]).alias("country_id"),
            f.max(df_countries["title"]).alias("country_title"),
            f.max(df_countries["country_short_title"]),
            f.avg(1 / df_currency_rates["rate"]).alias("usd_rate"),
            f.avg(df_currency_rates["rate"]).alias("usd_value"),
        )
        .select(
            f.lit("quarter").alias("time_period"),
            "date_id",
            "country_id",
            "country_title",
            "country_short_title",
            df_currency_rates["to_currency_id"].alias("currency_id"),
            df_currencies["short_title"].alias("currency_title"),
            "usd_rate",
            "usd_value",
        )
    )

    df_dim_currencies_month = (
        df_currency_rates.withColumn(
            "date_id",
            (
                f.concat(
                    f.date_format(df_currency_rates["considered_at"], "yyyyMM"),
                    f.lit("01"),
                )
            ).cast(IntegerType()),
        )
        .join(
            df_currencies,
            df_currencies["id"] == df_currency_rates["to_currency_id"],
            "inner",
        )
        .join(
            df_countries,
            df_countries["default_currency_id"] == df_currencies["id"],
            "inner",
        )
        .groupBy(
            "date_id",
            df_currency_rates["from_currency_id"],
            df_currency_rates["to_currency_id"],
            df_currencies["short_title"],
            df_countries["country_short_title"],
        )
        .agg(
            f.max(df_countries["id"]).alias("country_id"),
            f.max(df_countries["title"]).alias("country_title"),
            f.max(df_countries["country_short_title"]),
            f.avg(1 / df_currency_rates["rate"]).alias("usd_rate"),
            f.avg(df_currency_rates["rate"]).alias("usd_value"),
        )
        .select(
            f.lit("month").alias("time_period"),
            "date_id",
            "country_id",
            "country_title",
            "country_short_title",
            df_currency_rates["to_currency_id"].alias("currency_id"),
            df_currencies["short_title"].alias("currency_title"),
            "usd_rate",
            "usd_value",
        )
    )

    df_currency_rates_month_end = (
        df_currency_rates.withColumn(
            "date_id",
            f.date_format(f.col("considered_at"), "yyyyMMdd").cast(IntegerType()),
        )
        .filter(
            f.col("date_id")
            == (
                f.date_format(
                    f.last_day(df_currency_rates["considered_at"]), "yyyyMMdd"
                ).cast(IntegerType())
            )
        )
        .groupBy(
            "date_id",
            df_currency_rates["to_currency_id"],
            df_currency_rates["from_currency_id"],
        )
        .agg(f.max(f.col("id")).alias("id"))
        .select(f.col("id"), f.col("date_id"))
    )

    df_dim_currencies_month_end = (
        df_currency_rates_month_end.join(
            df_currency_rates,
            df_currency_rates_month_end["id"] == df_currency_rates["id"],
            "inner",
        )
        .join(
            df_currencies,
            df_currencies["id"] == df_currency_rates["to_currency_id"],
            "inner",
        )
        .join(
            df_countries,
            df_countries["default_currency_id"] == df_currencies["id"],
            "inner",
        )
        .select(
            f.lit("month_end").alias("time_period"),
            df_currency_rates_month_end["date_id"],
            df_countries["id"].alias("country_id"),
            df_countries["title"].alias("country_title"),
            df_countries["country_short_title"],
            df_currency_rates["to_currency_id"].alias("currency_id"),
            df_currencies["short_title"].alias("currency_title"),
            (1 / df_currency_rates["rate"]).alias("usd_rate"),
            df_currency_rates["rate"].alias("usd_value"),
        )
    )

    df_dim_currencies = df_dim_currencies_quarter.union(df_dim_currencies_month).union(
        df_dim_currencies_month_end
    )

    df_dim_currencies = jobExec.select_dataframe_columns(
        spark, df_dim_currencies, table_columns
    )
    df_dim_currencies = df_dim_currencies.repartition(num_partitions, "date_id")

    df_dim_currencies.write.insertInto(
        "{}.{}".format(jobExec.target_schema, jobExec.target_table),
        overwrite=True,
    )

    jobExec.totalLines = (
        spark.table("{}.{}".format(jobExec.target_schema, jobExec.target_table))
    ).count()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName(job_name)
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
