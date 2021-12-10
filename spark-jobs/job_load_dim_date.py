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

    df_date_templates = (
        spark.table("{}.date_templates".format(jobExec.database_replica_full))
        .select("country_id", "date_index", f.col("started_at").alias("datetime"))
        .filter(f.col("date_index") == 0)
        .filter(f.col("country_id") == 76)
    )

    df_dim_date_days = (
        df_date_templates.withColumn(
            "id", f.date_format(f.col("datetime"), "yyyyMMdd").cast("int")
        )
        .withColumn("year", f.year(f.col("datetime")))
        .withColumn("month", f.month(f.col("datetime")))
        .withColumn("year_month", f.col("year") * 100 + f.col("month"))
        .withColumn("day", f.dayofmonth(f.col("datetime")))
        .withColumn("quarter", f.quarter(f.col("datetime")))
        .withColumn("day_of_week", f.dayofweek(f.col("datetime")))
        .withColumn("day_of_year", f.dayofyear(f.col("datetime")))
        .withColumn("month_week_number", f.date_format(f.col("datetime"), "W"))
        .withColumn("year_week_number", f.weekofyear(f.col("datetime")))
        .withColumn(
            "first_day_of_month",
            f.date_format(f.date_trunc("month", f.col("datetime")), "yyyyMMdd").cast(
                "int"
            ),
        )
        .withColumn(
            "last_day_of_month",
            f.date_format(f.last_day(f.col("datetime")), "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "last_day_of_last_month_datetime",
            f.date_sub(f.date_trunc("month", f.col("datetime")), 1),
        )
        .withColumn(
            "first_day_of_last_month",
            f.date_format(
                f.date_trunc("month", f.col("last_day_of_last_month_datetime")),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "last_day_of_last_month",
            f.date_format(f.col("last_day_of_last_month_datetime"), "yyyyMMdd").cast(
                "int"
            ),
        )
        .withColumn(
            "first_day_of_week",
            f.date_format(f.expr("date_add(datetime, 1-day_of_week)"), "yyyyMMdd").cast(
                "int"
            ),
        )
        .withColumn(
            "last_day_of_week",
            f.date_format(f.expr("date_add(datetime, 7-day_of_week)"), "yyyyMMdd").cast(
                "int"
            ),
        )
        .withColumn(
            "next_sunday",
            f.date_format(f.expr("date_add(datetime, 8-day_of_week)"), "yyyyMMdd").cast(
                "int"
            ),
        )
    )

    df_dim_date_months = (
        df_dim_date_days.filter(f.col("day") == 1)
        .withColumn("id", f.col("id") - 1)
        .withColumn("day", f.lit(0))
        .withColumn("day_of_week", f.lit(0))
        .withColumn("day_of_year", f.lit(0))
        .withColumn("month_week_number", f.lit(0))
        .withColumn("year_week_number", f.lit(0))
        .withColumn("next_sunday", f.lit(None))
    )

    df_dim_date = df_dim_date_days.union(df_dim_date_months).select(
        "id",
        "year",
        "month",
        "year_month",
        "day",
        "quarter",
        "day_of_week",
        "day_of_year",
        "month_week_number",
        "year_week_number",
        "first_day_of_month",
        "last_day_of_month",
        "first_day_of_last_month",
        "last_day_of_last_month",
        "first_day_of_week",
        "last_day_of_week",
        "next_sunday",
    )

    df_dim_date = jobExec.select_dataframe_columns(spark, df_dim_date, table_columns)
    df_dim_date.write.insertInto(
        "{}.{}".format(jobExec.target_schema, jobExec.target_table),
        overwrite=True,
    )

    jobExec.totalLines = (
        spark.table("{}.{}".format(jobExec.target_schema, jobExec.target_table))
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
