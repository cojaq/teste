import os
from datetime import timedelta

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, IntegerType
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

    reference_date_30_days = int(
        (
            date_utils.convert_text_to_date(jobExec.reference_date, "%Y%m%d")
            - timedelta(30)
        ).strftime("%Y%m%d")
    )

    # Reading source table from ODS MariaDB
    df_gyms = spark.table(f"{jobExec.database_replica_full}.gyms").select(
        "id",
        "title",
        "premium",
        "gym_network_id",
        "status",
        "full_address",
        "postal_code",
        "neighborhood",
        "sublocality",
        "city",
        "state",
        "state_code",
        "country_id",
        "latitude",
        "longitude",
        "min_daily_value",
        "min_monthly_value",
        "contract_signed_at",
        "seller_id",
        "recorder_id",
        "trainer_id",
        "manager_id",
        "enabler_id",
        "disabler_id",
        "enabled_at",
        "disabled_at",
        "trained_at",
        "max_daily_value",
        "max_monthly_value",
        "country_id",
        "currency_id",
        "enabled_1",
        "enabled_2",
        "disable_b2c",
        "checkin_status",
        "relationship_manager_id",
        "open_sundays",
        "open_24h",
        "personal_trainer",
        "wellness_app",
    )

    df_referrals = spark.table(f"{jobExec.database_replica_full}.referrals").select(
        "gym_id", "created_at"
    )

    df_gym_classes = spark.table(f"{jobExec.database_replica_full}.gym_classes").select(
        "gym_id", "gym_product_id", "enabled_1", "enabled"
    )

    df_gym_products = spark.table(
        f"{jobExec.database_replica_full}.gym_products"
    ).select(
        "id",
        "gym_id",
        "max_monthly_commission_value",
        "product_id",
        "enabled",
    )

    df_stg_gym_visits = spark.table(f"{jobExec.database_work}.stg_gym_visits").select(
        "gym_id", "considered_at_converted", "considered_day"
    )

    df_countries = spark.table(f"{jobExec.database_replica_full}.countries").select(
        f.col("id").alias("country_id"), "timezone"
    )

    df_attribs = spark.table(f"{jobExec.database_replica_full}.attribs").select(
        "id", "attrib_category_id"
    )

    df_attrib_category_translations = (
        spark.table(f"{jobExec.database_replica_full}.attrib_category_translations")
        .filter(f.col("locale") == "en")
        .select("attrib_category_id", "title")
    )

    df_gym_attribs = spark.table(f"{jobExec.database_replica_full}.gym_attribs").select(
        "id", "attrib_id", "gym_id", "attrib_type", "enabled"
    )

    df_person_transactions = spark.table(
        f"{jobExec.database_replica_full}.person_transactions"
    ).select("considered_at", "gym_id")

    df_gym_networks = spark.table(
        f"{jobExec.database_replica_full}.gym_networks"
    ).select(
        f.col("id").alias("gym_network_id"),
        f.col("title").alias("gym_network_name"),
    )

    df_gyms.cache()

    df_gym_last_visit = (
        df_gyms.join(
            df_stg_gym_visits,
            [
                df_gyms["id"] == df_stg_gym_visits["gym_id"],
                df_stg_gym_visits["considered_day"] >= reference_date_30_days,
            ],
        )
        .select(
            "gym_id",
            f.lit(1).cast(IntegerType()).alias("gym_last_visit_30_days"),
        )
        .distinct()
    )

    df_is_booking_enabled = (
        df_gyms.withColumnRenamed("id", "gym_id")
        .filter((f.col("enabled") == 1) & (f.col("status") == 5))
        .join(df_gym_classes.filter(f.col("enabled_1") == 1), "gym_id", "inner")
        .join(
            df_gym_products.filter(f.col("enabled") == 1).select(
                f.col("id").alias("gym_product_id"), "enabled"
            ),
            "gym_product_id",
            "inner",
        )
        .select(
            "gym_id",
            f.lit(True).cast(BooleanType()).alias("is_booking_enabled"),
        )
        .distinct()
    )

    df_monthly_cap = (
        df_gyms.withColumnRenamed("id", "gym_id")
        .join(
            df_gym_products.filter(
                (f.col("product_id").isin(16, 17, 18)) & (f.col("enabled") == 1)
            ),
            "gym_id",
            "inner",
        )
        .groupBy("gym_id")
        .agg(
            f.max("max_monthly_commission_value").alias("monthly_cap"),
            f.min("max_monthly_commission_value").alias("min_monthly_cap"),
        )
    )

    df_referral_day = (
        df_gyms.join(
            df_referrals,
            [
                df_gyms["id"] == df_referrals["gym_id"],
                df_gyms["contract_signed_at"] > df_referrals["created_at"],
            ],
            "inner",
        )
        .groupBy("gym_id")
        .agg(
            f.min(
                f.date_format(df_referrals["created_at"], "yyyyMMdd").cast(
                    IntegerType()
                )
            ).alias("referral_day")
        )
    )

    df_attribs_category_lookup = (
        df_gym_attribs.filter(f.col("attrib_type") == 10)
        .join(
            df_attribs.withColumnRenamed("id", "attrib_id"),
            "attrib_id",
            "inner",
        )
        .join(
            df_attrib_category_translations.filter(f.col("locale") == "en"),
            "attrib_category_id",
            "inner",
        )
        .select(
            df_gym_attribs["id"],
            "gym_id",
            df_attrib_category_translations["title"],
            df_gym_attribs["enabled"],
        )
    )

    df_first_gym_category = df_attribs_category_lookup.join(
        df_attribs_category_lookup.groupBy("gym_id").agg(f.min("id").alias("id")),
        ["gym_id", "id"],
        "inner",
    ).select("gym_id", f.col("title").alias("first_gym_category"))

    df_concat_gym_category = (
        df_attribs_category_lookup.filter("enabled == 1")
        .groupBy("gym_id")
        .agg(f.concat_ws("|", f.collect_set("title")).alias("concat_gym_category"))
    )

    df_last_gym_visit_at = df_person_transactions.groupBy("gym_id").agg(
        f.max("considered_at").alias("last_gym_visit_at")
    )

    df_gyms = (
        df_gyms.join(
            df_person_transactions.select("gym_id").distinct(),
            df_gyms["id"] == df_person_transactions["gym_id"],
            "left",
        )
        .filter(
            (f.col("status").isin(9, 10, 12) == False) | (f.col("gym_id").isNotNull())
        )
        .drop("gym_id")
    )

    df_dim_gyms = (
        df_gyms.withColumnRenamed("id", "gym_id")
        .join(df_monthly_cap, "gym_id", "left")
        .join(df_referral_day, "gym_id", "left")
        .join(df_is_booking_enabled, "gym_id", "left")
        .join(df_gym_last_visit, "gym_id", "left")
        .join(df_countries, "country_id", "left")
        .join(df_first_gym_category, "gym_id", "left")
        .join(df_concat_gym_category, "gym_id", "left")
        .join(df_last_gym_visit_at, "gym_id", "left")
        .join(df_gym_networks, "gym_network_id", "left")
        .withColumn(
            "gym_status",
            f.when((f.col("status") == 5) & (f.col("enabled_1") == 0), "TRAINING")
            .when(
                (f.col("status") == 5)
                & (f.col("enabled_1") == 1)
                & (f.col("gym_last_visit_30_days") == 1),
                "ACTIVE LIVE",
            )
            .when(
                (f.col("status") != 5)
                & (f.col("enabled_1") == 0)
                & (f.col("gym_last_visit_30_days") == 1),
                "ACTIVE",
            )
            .when(
                (f.col("status") == 5)
                & (f.col("enabled_1") == 1)
                & (f.col("gym_last_visit_30_days").isNull()),
                "LIVE NOT ACTIVE",
            )
            .when(
                (f.col("status") != 5)
                & (f.col("enabled_1") == 0)
                & (f.col("gym_last_visit_30_days").isNull()),
                "INACTIVE",
            ),
        )
        .withColumn(
            "validation_type",
            f.when(f.col("checkin_status") == 0, "ALL")
            .when(f.col("checkin_status") == 10, "MIGRATING TO CHECKIN")
            .when(f.col("checkin_status") == 20, "CHECKIN ONLY")
            .when(f.col("checkin_status") == 30, "SMART CHECKIN")
            .otherwise("OTHERS"),
        )
        .withColumn(
            "enabled_day",
            (
                f.date_format(
                    f.expr("from_utc_timestamp(enabled_at, timezone)"),
                    "yyyyMMdd",
                )
            ).cast(IntegerType()),
        )
        .withColumn(
            "disabled_day",
            (
                f.date_format(
                    f.expr("from_utc_timestamp(disabled_at, timezone)"),
                    "yyyyMMdd",
                )
            ).cast(IntegerType()),
        )
        .select(
            "gym_id",
            f.col("title").alias("gym_name"),
            "gym_network_id",
            "gym_network_name",
            f.col("status").alias("curr_status"),
            "enabled_1",
            "latitude",
            "longitude",
            "gym_status",
            f.col("premium").alias("premium_flag"),
            "country_id",
            "first_gym_category",
            "concat_gym_category",
            "open_sundays",
            "open_24h",
            "validation_type",
            "min_monthly_value",
            "monthly_cap",
            "min_monthly_cap",
            f.coalesce("is_booking_enabled", f.lit(False).cast(BooleanType())).alias(
                "is_booking_enabled"
            ),
            "seller_id",
            "full_address",
            "postal_code",
            "neighborhood",
            "sublocality",
            "city",
            "state",
            "state_code",
            f.col("personal_trainer").alias("is_personal_trainer"),
            f.col("wellness_app").alias("is_wellness_app"),
        )
    )

    df_dim_gyms = jobExec.select_dataframe_columns(spark, df_dim_gyms, table_columns)
    df_dim_gyms = df_dim_gyms.repartition(num_partitions, "gym_id")

    df_dim_gyms.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
