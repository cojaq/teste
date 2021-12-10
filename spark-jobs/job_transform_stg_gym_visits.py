import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType, IntegerType, ShortType, StringType
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
    df_person_transactions = spark.table(
        f"{jobExec.database_replica_full}.person_transactions"
    ).select(
        "id",
        "person_product_id",
        "person_token_id",
        "gym_product_id",
        "company_id",
        "country_id",
        "considered_at",
        "considered_at_converted",
        "considered_day",
        "considered_hour",
        "gym_id",
        "considered_unit_price",
        "unit_price_before_discount",
        "gym_commission",
        "gym_commission_percentage",
        "total_refunded_value",
        "accumulated_uses",
        "considered_at",
        "created_at",
        "refunded_at",
        "created_at_converted",
        "refunded_at_converted",
        f.col("product_id").alias("person_transactions_product_id"),
    )

    df_person_products = spark.table(
        f"{jobExec.database_replica_full}.person_products"
    ).select(
        "id",
        "person_cart_id",
        "bu",
        "site_id",
        "country_id",
        "company_id",
        "parent_company_id",
        "person_id",
        "company_member_id",
        "product_id",
        "parent_plan_id",
        "report_id",
        "gym_slot_id",
        "gym_product_id",
        "unit_quantity",
    )

    df_person_carts = spark.table(
        f"{jobExec.database_replica_full}.person_carts"
    ).select(
        "id",
        "company_id",
        "total_price_before_conditions",
        "person_payment_method_id",
    )

    df_person_tokens = spark.table(
        f"{jobExec.database_replica_full}.person_tokens"
    ).select("id", "checked_in_at", "token_type")

    df_person_cart_conditions = spark.table(
        f"{jobExec.database_replica_full}.person_cart_conditions"
    ).select("id", "person_cart_id", "condition_id")

    df_conditions = spark.table(f"{jobExec.database_replica_full}.conditions").select(
        "id", "auto_condition_type"
    )

    df_companies = spark.table(f"{jobExec.database_replica_full}.companies").select(
        "id",
        "parent_company_id",
        "parent_group_company_id",
        "company_type",
        "person_monthly_discount_value",
    )

    df_company_plans = spark.table(
        f"{jobExec.database_replica_full}.company_plans"
    ).select("id", "person_monthly_discount_value", "company_id")

    df_person_payment_methods = spark.table(
        f"{jobExec.database_replica_full}.person_payment_methods"
    ).select("id", "payment_methodable_type")

    df_gym_products = spark.table(
        f"{jobExec.database_replica_full}.gym_products"
    ).select("id", "monthly_value")

    df_parent_person_products = (
        df_person_products.select(
            f.col("id").alias("person_product_id"),
            f.coalesce(f.col("parent_plan_id"), f.col("id")).alias("parent_plan_id"),
            "person_cart_id",
            "person_id",
            "gym_product_id",
            "product_id",
            "report_id",
        )
        .join(
            df_person_products.select(
                f.col("id").alias("parent_plan_person_product_id"),
                f.col("person_cart_id").alias("parent_plan_person_cart_id"),
                f.col("product_id").alias("parent_plan_product_id"),
                f.col("report_id").alias("parent_plan_report_id"),
                "bu",
                "site_id",
                "company_id",
                "company_member_id",
                "gym_slot_id",
                "unit_quantity",
                f.coalesce(f.col("parent_company_id"), f.col("company_id")).alias(
                    "parent_company_id"
                ),
            ),
            f.col("parent_plan_person_product_id") == f.col("parent_plan_id"),
            "inner",
        )
        .select(
            "person_product_id",
            "person_cart_id",
            "person_id",
            "product_id",
            "report_id",
            f.col("gym_product_id").alias("person_products_gym_product_id"),
            "bu",
            "site_id",
            f.col("company_id").alias("person_products_company_id"),
            "company_member_id",
            "gym_slot_id",
            "unit_quantity",
            f.col("parent_company_id").alias("person_products_parent_company_id"),
            "parent_plan_person_product_id",
            "parent_plan_person_cart_id",
            "parent_plan_product_id",
            "parent_plan_report_id",
        )
    )

    df_parent_companies = (
        df_companies.select(
            f.col("id").alias("company_id"),
            f.coalesce(f.col("parent_company_id"), f.col("id")).alias(
                "parent_company_id"
            ),
            "company_type",
            "person_monthly_discount_value",
        )
        .join(
            df_companies.withColumn(
                "parent_group_company_id",
                f.coalesce(
                    df_companies["parent_group_company_id"],
                    f.coalesce(df_companies["parent_company_id"], df_companies["id"]),
                ),
            ).select(
                f.col("id").alias("parent_company_id"),
                "parent_group_company_id",
            ),
            "parent_company_id",
            "left",
        )
        .select(
            "company_id",
            "company_type",
            "person_monthly_discount_value",
            "parent_group_company_id",
        )
    )

    df_person_cart_conditions = (
        df_person_cart_conditions.join(
            df_conditions.filter(df_conditions["auto_condition_type"] == 30).select(
                df_conditions["id"].alias("condition_id")
            ),
            "condition_id",
            "inner",
        )
        .select(
            "person_cart_id",
            f.lit(1).cast(ShortType()).alias("has_copay_discount"),
        )
        .distinct()
    )

    # Join condition with table company_plans
    join_condition = [
        df_parent_person_products["person_products_company_id"]
        == df_company_plans["company_id"],
        f.round(
            df_person_carts["total_price_before_conditions"]
            / df_parent_person_products["unit_quantity"],
            0,
        )
        >= df_company_plans["person_monthly_discount_value"],
    ]

    df_plan_value = (
        df_person_carts.withColumnRenamed("id", "person_cart_id")
        .select("person_cart_id", "total_price_before_conditions")
        .join(
            df_parent_person_products.select(
                f.col("parent_plan_person_cart_id").alias("person_cart_id"),
                "unit_quantity",
                "person_products_company_id",
                f.col("parent_plan_person_product_id"),
            ),
            "person_cart_id",
            "inner",
        )
        .join(
            df_company_plans.select("person_monthly_discount_value", "company_id"),
            join_condition,
            "left",
        )
        .groupBy("parent_plan_person_product_id")
        .agg(
            f.max("person_monthly_discount_value").alias(
                "person_monthly_discount_value"
            )
        )
        .select(
            "parent_plan_person_product_id",
            f.col("person_monthly_discount_value").cast(DecimalType(12, 2)),
        )
    )

    df_stg_gym_visits = (
        df_person_transactions.filter(df_person_transactions["refunded_at"].isNull())
        .withColumnRenamed("id", "person_transaction_id")
        .join(df_parent_person_products, "person_product_id", "inner")
        .join(
            df_person_carts.withColumnRenamed("id", "parent_plan_person_cart_id"),
            "parent_plan_person_cart_id",
            "inner",
        )
        .join(
            df_person_tokens.withColumnRenamed("id", "person_token_id"),
            "person_token_id",
            "left",
        )
        .join(
            df_gym_products.withColumnRenamed("id", "gym_product_id"),
            "gym_product_id",
            "left",
        )
        .join(df_parent_companies, "company_id", "left")
        .join(df_person_cart_conditions, "person_cart_id", "left")
        .join(
            df_person_payment_methods.withColumnRenamed(
                "id", "person_payment_method_id"
            ),
            "person_payment_method_id",
            "left",
        )
        .join(df_plan_value, "parent_plan_person_product_id", "left")
        .withColumn(
            "has_copay_discount",
            f.coalesce(
                df_person_cart_conditions["has_copay_discount"],
                f.lit(0).cast(ShortType()),
            ),
        )
        .withColumn(
            "payment_method_type",
            f.when(
                (
                    (
                        (df_companies["company_type"] == 50)
                        & (
                            df_person_payment_methods[
                                "payment_methodable_type"
                            ].isNull()
                        )
                    )
                    | (df_companies["company_type"] == 45)
                ),
                f.lit("RemotePersonCard"),
            )
            .when(
                df_companies["company_type"] == 42,
                f.lit("Payroll").cast(StringType()),
            )
            .otherwise(f.lit("RemotePersonCard")),
        )
        .withColumn(
            "plan_value",
            f.when(
                df_companies["company_type"] != 50,
                df_companies["person_monthly_discount_value"].cast(DecimalType(12, 2)),
            ).otherwise(df_plan_value["person_monthly_discount_value"]),
        )
        .withColumn(
            "checked_in_at",
            (f.date_format(df_person_tokens["checked_in_at"], "yyyyMMdd")).cast(
                IntegerType()
            ),
        )
        .withColumn(
            "token_type",
            f.coalesce(f.col("token_type"), f.lit(-1).cast(IntegerType())),
        )
        .withColumn(
            "token_group_type",
            f.when(f.col("checked_in_at").isNotNull(), f.lit("Check-in"))
            .when(
                f.coalesce("token_type", f.lit(-1).cast(IntegerType())).isin(
                    -1, 0, 10, 15, 20, 35, 40
                ),
                f.lit("Standard"),
            )
            .when(
                f.coalesce("token_type", f.lit(-1).cast(IntegerType())) == 30,
                f.lit("SMS"),
            )
            .otherwise(f.lit("Undefined")),
        )
        .withColumn(
            "retroactive_validation",
            f.when(
                (
                    (
                        f.round(
                            (
                                f.unix_timestamp(df_person_transactions["created_at"])
                                - f.unix_timestamp(
                                    df_person_transactions["considered_at"]
                                )
                            )
                            / 3600
                        )
                    ).cast(IntegerType())
                    > 24
                )
                & (df_person_transactions["person_transactions_product_id"] == 16),
                f.lit(1).cast(ShortType()),
            ).otherwise(f.lit(0).cast(ShortType())),
        )
        .select(
            f.col("person_product_id").alias("gym_visit_person_product_id"),
            f.col("person_cart_id").alias("gym_visit_person_cart_id"),
            f.col("person_transaction_id"),
            f.col("parent_plan_person_product_id").alias("person_product_id"),
            f.col("parent_plan_person_cart_id").alias("person_cart_id"),
            f.col("bu"),
            f.col("site_id"),
            f.col("country_id"),
            f.col("person_products_company_id").alias("company_id"),
            f.col("person_products_parent_company_id").alias("parent_company_id"),
            f.col("parent_group_company_id"),
            f.col("person_id"),
            f.col("company_member_id"),
            f.col("plan_value"),
            f.col("payment_method_type"),
            f.col("has_copay_discount"),
            f.col("considered_at"),
            f.col("considered_at_converted"),
            f.col("considered_day"),
            f.col("considered_hour"),
            f.col("checked_in_at"),
            f.col("person_products_gym_product_id").alias("gym_product_id"),
            f.col("monthly_value").alias("gym_product_value"),
            f.col("product_id"),
            f.col("report_id"),
            f.col("parent_plan_report_id"),
            f.col("parent_plan_product_id"),
            f.col("parent_plan_person_product_id"),
            f.col("parent_plan_person_cart_id"),
            f.col("gym_id"),
            f.col("considered_unit_price"),
            f.col("unit_price_before_discount"),
            f.col("gym_commission"),
            f.col("gym_commission_percentage"),
            f.col("total_refunded_value"),
            f.col("accumulated_uses"),
            f.col("token_type"),
            f.col("token_group_type"),
            f.col("gym_slot_id"),
            f.col("retroactive_validation"),
            f.col("created_at_converted"),
            f.col("refunded_at_converted"),
            f.col("person_transactions_product_id"),
        )
    )

    df_stg_gym_visits = jobExec.select_dataframe_columns(
        spark, df_stg_gym_visits, table_columns
    )
    df_stg_gym_visits = df_stg_gym_visits.repartition(
        num_partitions,
        ["gym_visit_person_product_id", "gym_visit_person_cart_id"],
    )

    df_stg_gym_visits.write.insertInto(
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
