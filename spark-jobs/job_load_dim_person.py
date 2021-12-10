import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, ShortType
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
    df_company_members = spark.table(
        f"{jobExec.database_replica_full}.company_members"
    ).select(
        f.col("id").alias("person_company_id"),
        "company_id",
        "person_id",
        "first_name",
        "last_name",
        "email",
        "gender",
        "has_phone",
        "created_at_converted",
        "disabled_at_converted",
        "first_gym_visit",
        "first_purchase",
        "first_sign_in",
        "parent_person_id",
        "department",
        "subdepartment",
        "subdepartment_2",
        "subdepartment_3",
    )
    df_company_members.cache()

    df_stg_carts_products = spark.table(
        f"{jobExec.database_work}.stg_carts_products"
    ).select("person_id", "paid_at_converted")

    df_stg_page_views = spark.table(f"{jobExec.database_work}.stg_page_views")

    df_stg_gym_visits = spark.table(f"{jobExec.database_work}.stg_gym_visits")

    df_dim_companies = spark.table(f"{jobExec.target_schema}.v_dim_companies").select(
        "company_id", "country_id", "family_member"
    )

    df_people = spark.table(f"{jobExec.database_replica_full}.people").select(
        f.col("id").alias("person_id"), "token", "referred_by_token"
    )
    df_people.cache()

    # Creating dataframe with column first_sign_in
    # Filter null because the source query treats the first_sign_in to the first company
    df_company_members_first_sign_in = (
        df_company_members.filter(df_company_members["first_sign_in"].isNull())
        .join(
            df_stg_page_views,
            [
                df_company_members["person_id"] == df_stg_page_views["person_id"],
                df_company_members["created_at_converted"]
                <= df_stg_page_views["created_at_converted"],
                f.coalesce(
                    df_company_members["disabled_at_converted"],
                    f.lit("2100-12-31"),
                )
                >= df_stg_page_views["created_at_converted"],
            ],
            "inner",
        )
        .groupBy(df_company_members["person_company_id"])
        .agg(
            f.min(df_stg_page_views["created_at_converted"]).alias(
                "first_sign_in_day_at_local"
            )
        )
        .select(
            df_company_members["person_company_id"],
            "first_sign_in_day_at_local",
        )
    )

    # Creating dataframe with column first_purchase
    df_company_members_first_purchase = (
        df_company_members.filter(df_company_members["first_purchase"].isNull())
        .join(
            df_stg_carts_products,
            [
                df_company_members["person_id"] == df_stg_carts_products["person_id"],
                df_company_members["created_at_converted"]
                <= df_stg_carts_products["paid_at_converted"],
                f.coalesce(
                    df_company_members["disabled_at_converted"],
                    f.lit("2100-12-31"),
                )
                >= df_stg_carts_products["paid_at_converted"],
            ],
            "inner",
        )
        .groupBy(df_company_members["person_company_id"])
        .agg(
            f.min(df_stg_carts_products["paid_at_converted"]).alias(
                "first_purchase_day_at_local"
            )
        )
        .select(
            df_company_members["person_company_id"],
            "first_purchase_day_at_local",
        )
    )

    # Creating dataframe with column first_gym_visit
    df_company_members_first_gym_visit = (
        df_company_members.filter(df_company_members["first_gym_visit"].isNull())
        .join(
            df_stg_gym_visits,
            [
                df_company_members["person_id"] == df_stg_gym_visits["person_id"],
                df_company_members["created_at_converted"]
                <= df_stg_gym_visits["considered_at_converted"],
                f.coalesce(
                    df_company_members["disabled_at_converted"],
                    f.lit("2100-12-31"),
                )
                >= df_stg_gym_visits["considered_at_converted"],
            ],
            "inner",
        )
        .groupBy(df_company_members["person_company_id"])
        .agg(
            f.min(df_stg_gym_visits["considered_at_converted"]).alias(
                "first_gym_visit_day_at_local"
            )
        )
        .select(
            df_company_members["person_company_id"],
            "first_gym_visit_day_at_local",
        )
    )

    df_sub_people = df_people.select("person_id", "token")
    df_company_members_referral = (
        df_company_members.filter(
            f.col("parent_person_id").isNull() | (f.col("parent_person_id") == "")
        )
        .join(
            df_people.filter(
                df_people["referred_by_token"].isNotNull()
                & (df_people["referred_by_token"] != "")
            ),
            [df_company_members["person_id"] == df_people["person_id"]],
            "inner",
        )
        .select("person_company_id", "referred_by_token")
        .join(
            df_sub_people,
            [f.col("referred_by_token") == df_sub_people["token"]],
            "inner",
        )
        .select(
            "person_company_id",
            f.col("person_id").alias("referral_parent_person_id"),
        )
    )

    df_dim_person = (
        df_company_members.join(
            df_company_members_first_sign_in, "person_company_id", "left"
        )
        .join(df_company_members_first_purchase, "person_company_id", "left")
        .join(df_company_members_first_gym_visit, "person_company_id", "left")
        .join(df_company_members_referral, "person_company_id", "left")
        .withColumn(
            "first_sign_in_day",
            f.when(
                df_company_members["first_sign_in"].isNull(),
                f.date_format(
                    df_company_members_first_sign_in["first_sign_in_day_at_local"],
                    "yyyyMMdd",
                ).cast(IntegerType()),
            ).otherwise(
                (f.date_format(df_company_members["first_sign_in"], "yyyyMMdd")).cast(
                    IntegerType()
                )
            ),
        )
        .withColumn(
            "first_sign_in_at_local",
            f.when(
                df_company_members["first_sign_in"].isNull(),
                df_company_members_first_sign_in["first_sign_in_day_at_local"],
            ).otherwise(df_company_members["first_sign_in"]),
        )
        .withColumn(
            "first_purchase_day",
            f.when(
                df_company_members["first_purchase"].isNull(),
                f.date_format(
                    df_company_members_first_purchase["first_purchase_day_at_local"],
                    "yyyyMMdd",
                ).cast(IntegerType()),
            ).otherwise(
                (f.date_format(df_company_members["first_purchase"], "yyyyMMdd")).cast(
                    IntegerType()
                )
            ),
        )
        .withColumn(
            "first_purchase_at_local",
            f.when(
                df_company_members["first_purchase"].isNull(),
                df_company_members_first_purchase["first_purchase_day_at_local"],
            ).otherwise(df_company_members["first_purchase"]),
        )
        .withColumn(
            "first_gym_visit_day",
            f.when(
                df_company_members["first_gym_visit"].isNull(),
                f.date_format(
                    df_company_members_first_gym_visit["first_gym_visit_day_at_local"],
                    "yyyyMMdd",
                ).cast(IntegerType()),
            ).otherwise(
                (f.date_format(df_company_members["first_gym_visit"], "yyyyMMdd")).cast(
                    IntegerType()
                )
            ),
        )
        .withColumn(
            "first_gym_visit_at_local",
            f.when(
                df_company_members["first_gym_visit"].isNull(),
                df_company_members_first_gym_visit["first_gym_visit_day_at_local"],
            ).otherwise(df_company_members["first_gym_visit"]),
        )
        .withColumn(
            "status",
            f.when(
                df_company_members["disabled_at_converted"].isNotNull(),
                f.when(
                    f.col("first_gym_visit_day").isNotNull(),
                    f.lit("disabled active"),
                )
                .when(
                    f.col("first_purchase_day").isNotNull(),
                    f.lit("disabled enrolled"),
                )
                .when(
                    f.col("first_sign_in_day").isNotNull(),
                    f.lit("disabled signed up"),
                )
                .otherwise(f.lit("disabled")),
            )
            .when(
                f.col("first_gym_visit_day").isNotNull(),
                f.lit("enrolled active"),
            )
            .when(f.col("first_purchase_day").isNotNull(), f.lit("enrolled"))
            .when(f.col("first_sign_in_day").isNotNull(), f.lit("signed up"))
            .otherwise(f.lit("eligible")),
        )
        .withColumn("recent_canceled_day", f.lit(0).cast(ShortType()))
        .withColumn(
            "parent_person_id",
            f.when(
                f.col("parent_person_id").isNotNull()
                & (f.col("parent_person_id") != ""),
                f.col("parent_person_id"),
            )
            .when(
                f.col("referral_parent_person_id").isNotNull(),
                f.col("referral_parent_person_id"),
            )
            .otherwise(f.lit(None)),
        )
        .withColumn(
            "assign_type",
            f.when(
                f.col("parent_person_id").isNotNull()
                & (f.col("parent_person_id") != ""),
                f.lit("FM invite"),
            )
            .when(
                f.col("referral_parent_person_id").isNotNull(),
                f.lit("referral"),
            )
            .otherwise(f.lit(None)),
        )
        .join(df_dim_companies, "company_id", "left")
        .select(
            "person_company_id",
            "person_id",
            "first_name",
            "last_name",
            f.col("email").alias("email_address"),
            "gender",
            "country_id",
            f.col("has_phone").alias("has_phone_number"),
            "status",
            "company_id",
            f.col("created_at_converted").alias("created_at_local"),
            f.date_format(f.col("created_at_converted"), "yyyMMdd").alias(
                "created_day"
            ),
            f.col("disabled_at_converted").alias("disabled_at_local"),
            f.date_format(f.col("disabled_at_converted"), "yyyMMdd").alias(
                "disabled_day"
            ),
            "recent_canceled_day",
            "first_sign_in_day",
            "first_sign_in_at_local",
            "first_purchase_day",
            "first_purchase_at_local",
            "first_gym_visit_day",
            "first_gym_visit_at_local",
            f.coalesce(f.col("family_member"), f.lit(False)).alias("family_member"),
            "parent_person_id",
            "assign_type",
            "department",
            "subdepartment",
            "subdepartment_2",
            "subdepartment_3",
        )
    )

    df_dim_person = jobExec.select_dataframe_columns(
        spark, df_dim_person, table_columns
    )
    df_dim_person = df_dim_person.repartition(
        num_partitions, ["person_company_id", "person_id"]
    )

    df_dim_person.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
