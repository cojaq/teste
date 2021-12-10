import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
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
    # Reading source table
    df_v_dim_person = spark.table("{}.v_dim_person".format(jobExec.target_schema))

    df_v_dim_date = spark.table("{}.v_dim_date".format(jobExec.target_schema))

    df_v_dim_companies = spark.table("{}.v_dim_companies".format(jobExec.target_schema))

    # Transform

    df_v_dim_date_monthly = (
        df_v_dim_date.groupBy(f.col("year_month"))
        .agg(
            f.min(f.col("first_day_of_month")).alias("first_day_of_month"),
            f.max(f.col("last_day_of_month")).alias("last_day_of_month"),
        )
        .select("year_month", "first_day_of_month", "last_day_of_month")
    )

    df_companies_eligibles_monthly = (
        df_v_dim_person.join(
            df_v_dim_companies,
            df_v_dim_person["company_id"] == df_v_dim_companies["company_id"],
            how="inner",
        )
        .join(
            df_v_dim_date_monthly,
            (
                (
                    df_v_dim_person["created_day"]
                    <= df_v_dim_date_monthly["last_day_of_month"]
                )
                & (
                    f.coalesce(df_v_dim_person["disabled_day"], f.lit(99999999))
                    >= df_v_dim_date_monthly["first_day_of_month"]
                )
            ),
            how="inner",
        )
        .groupBy(
            df_v_dim_date_monthly["first_day_of_month"],
            df_v_dim_companies["country_id"],
            df_v_dim_companies["parent_company_id"],
            df_v_dim_companies["family_member"],
        )
        .agg(
            f.max(df_v_dim_companies["parent_company_title"]).alias(
                "parent_company_title"
            ),
            f.max(f.coalesce(df_v_dim_companies["total_employees"])).alias(
                "company_size"
            ),
            f.countDistinct(df_v_dim_person["person_id"]).alias("eligibles_number"),
        )
        .select(
            f.lit("monthly").alias("segment"),
            f.col("first_day_of_month").alias("date"),
            "country_id",
            "parent_company_id",
            "family_member",
            "parent_company_title",
            "company_size",
            "eligibles_number",
        )
    )

    df_v_dim_date_weekly = (
        df_v_dim_date.filter(
            (
                f.col("id")
                <= f.date_format(f.current_date(), "yyyyMMdd").cast(IntegerType())
            )
            & (f.col("day") != 0)
        )
        .groupBy(f.col("last_day_of_week"))
        .agg(f.min(f.col("first_day_of_week")).alias("first_day_of_week"))
        .orderBy(f.col("last_day_of_week"), ascending=False)
        .select("first_day_of_week", "last_day_of_week")
        .limit(52)
    )

    df_companies_eligibles_weekly = (
        df_v_dim_person.join(
            df_v_dim_companies,
            df_v_dim_person["company_id"] == df_v_dim_companies["company_id"],
            how="inner",
        )
        .join(
            df_v_dim_date_weekly,
            (
                (
                    df_v_dim_person["created_day"]
                    <= df_v_dim_date_weekly["last_day_of_week"]
                )
                & (
                    f.coalesce(df_v_dim_person["disabled_day"], f.lit(99999999))
                    >= df_v_dim_date_weekly["first_day_of_week"]
                )
            ),
            how="inner",
        )
        .groupBy(
            df_v_dim_date_weekly["last_day_of_week"],
            df_v_dim_companies["country_id"],
            df_v_dim_companies["parent_company_id"],
            df_v_dim_companies["family_member"],
        )
        .agg(
            f.max(df_v_dim_companies["parent_company_title"]).alias(
                "parent_company_title"
            ),
            f.max(f.coalesce(df_v_dim_companies["total_employees"])).alias(
                "company_size"
            ),
            f.countDistinct(df_v_dim_person["person_id"]).alias("eligibles_number"),
        )
        .select(
            f.lit("weekly").alias("segment"),
            f.col("last_day_of_week").alias("date"),
            "country_id",
            "parent_company_id",
            "family_member",
            "parent_company_title",
            "company_size",
            "eligibles_number",
        )
    )

    df_companies_eligibles = df_companies_eligibles_monthly.union(
        df_companies_eligibles_weekly
    )

    df_companies_eligibles = jobExec.select_dataframe_columns(
        spark, df_companies_eligibles, table_columns
    )
    df_companies_eligibles = df_companies_eligibles.repartition(num_partitions)

    df_companies_eligibles.write.insertInto(
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
