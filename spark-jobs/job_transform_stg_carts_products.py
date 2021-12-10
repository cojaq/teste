import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DecimalType, ShortType, StringType
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
    df_person_products = (
        spark.table(f"{jobExec.database_replica_full}.person_products")
        .select(
            "id",
            "person_cart_id",
            "bu",
            "site_id",
            "country_id",
            "company_id",
            "person_id",
            "company_member_id",
            "product_id",
            "product_category_id",
            "gym_product_id",
            "gym_slot_id",
            "change_to_gym_product_id",
            "valid_start_date",
            "valid_end_date",
            "paid_at",
            "available_unit_quantity",
            "consumption_at",
            "refunded_at",
            "expiration_at",
            "gym_id",
            "current_installment",
            "unit_price",
            "product_price",
            "available_price",
            "unit_quantity",
            "valid_start_day",
            "valid_end_day",
        )
        .filter("product_id != 16 and paid_at is not null")
    )

    df_person_carts = (
        spark.table(f"{jobExec.database_replica_full}.person_carts")
        .select(
            "id",
            "company_id",
            "total_price_before_conditions",
            "person_id",
            "report_id",
            "person_payment_method_id",
            "payment_method",
            "country_id",
            "paid_at",
            "paid_at_converted",
            "refunded_at",
            "refunded_at_converted",
            "total_product_quantity",
            "total_revenue",
            "subsidy_value",
            "subsidy_to_gympass_value",
            "taxable_total_price",
            "total_refunded_value",
            "total_price",
            "receipt_id",
        )
        .filter("report_id != 16 and paid_at is not null")
    )

    df_person_cart_conditions = spark.table(
        f"{jobExec.database_replica_full}.person_cart_conditions"
    ).select("id", "person_cart_id", "condition_id")

    df_conditions = spark.table(f"{jobExec.database_replica_full}.conditions").select(
        "id", "auto_condition_type"
    )

    df_companies = spark.table(f"{jobExec.database_replica_full}.companies").select(
        "id", "company_type", "person_monthly_discount_value"
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
        df_person_products["company_id"] == df_company_plans["company_id"],
        f.round(
            df_person_carts["total_price_before_conditions"]
            / df_person_products["unit_quantity"],
            0,
        )
        >= df_company_plans["person_monthly_discount_value"],
    ]

    df_plan_value = (
        df_person_carts.withColumnRenamed("id", "person_cart_id")
        .select("person_cart_id", "total_price_before_conditions", "company_id")
        .join(
            df_person_products.select("person_cart_id", "unit_quantity", "company_id"),
            "person_cart_id",
            "inner",
        )
        .join(
            df_company_plans.select("person_monthly_discount_value", "company_id"),
            join_condition,
            "left",
        )
        .groupBy("person_cart_id")
        .agg(
            f.max("person_monthly_discount_value").alias(
                "person_monthly_discount_value"
            )
        )
        .select(
            "person_cart_id",
            f.col("person_monthly_discount_value").cast(DecimalType(12, 2)),
        )
    )

    df_stg_carts_products = (
        df_person_carts.withColumnRenamed("id", "person_cart_id")
        .join(
            df_person_products.withColumnRenamed("id", "person_product_id"),
            "person_cart_id",
            "inner",
        )
        .join(df_person_cart_conditions, "person_cart_id", "left")
        .join(
            df_companies.withColumnRenamed("id", "company_id"),
            "company_id",
            "left",
        )
        .join(
            df_person_payment_methods.withColumnRenamed(
                "id", "person_payment_method_id"
            ),
            "person_payment_method_id",
            "left",
        )
        .join(
            df_gym_products.withColumnRenamed("id", "gym_product_id"),
            "gym_product_id",
            "left",
        )
        .join(df_plan_value, "person_cart_id", "left")
        .withColumn(
            "has_copay_discount",
            f.when(
                df_person_cart_conditions["has_copay_discount"].isNull(),
                f.lit(0).cast(ShortType()),
            ).otherwise(df_person_cart_conditions["has_copay_discount"]),
        )
        .withColumn(
            "payment_method_type",
            f.when(df_companies["company_type"] == 42, f.lit("Payroll"))
            .when(df_companies["company_type"] == 45, f.lit("RemotePersonCard"))
            .when(
                (df_companies["company_type"] == 50)
                & (df_person_carts["receipt_id"].isNotNull()),
                f.lit("Payroll"),
            )
            .when(
                (df_companies["company_type"] == 50)
                & (df_person_carts["payment_method"] == "free"),
                f.lit("Free"),
            )
            .when(
                df_companies["company_type"] == 50,
                f.coalesce(
                    df_person_payment_methods["payment_methodable_type"],
                    f.lit("RemotePersonCard"),
                ),
            )
            .otherwise(f.lit("RemotePersonCard")),
        )
        .withColumn(
            "plan_value",
            f.when(
                (df_companies["company_type"] != 50)
                & (
                    df_companies["person_monthly_discount_value"].cast(
                        DecimalType(12, 2)
                    )
                    > 0
                ),
                df_companies["person_monthly_discount_value"].cast(DecimalType(12, 2)),
            ).otherwise(df_plan_value["person_monthly_discount_value"]),
        )
        .select(
            "person_cart_id",
            "person_product_id",
            "bu",
            "site_id",
            df_person_products["country_id"].alias("country_id"),
            df_person_products["company_id"].alias("company_id"),
            "company_type",
            "plan_value",
            df_person_products["person_id"].alias("person_id"),
            df_person_carts["person_id"].alias("purchase_person_id"),
            "company_member_id",
            "report_id",
            "product_id",
            "product_category_id",
            "gym_id",
            "monthly_value",
            "gym_slot_id",
            "change_to_gym_product_id",
            "payment_method_type",
            "valid_start_day",
            "valid_end_day",
            "current_installment",
            "paid_at_converted",
            "refunded_at_converted",
            "total_product_quantity",
            "unit_price",
            "product_price",
            "total_price",
            "available_price",
            "unit_quantity",
            "available_unit_quantity",
            "total_revenue",
            "subsidy_value",
            "subsidy_to_gympass_value",
            "taxable_total_price",
            "total_refunded_value",
            "has_copay_discount",
        )
    )

    df_stg_carts_products = jobExec.select_dataframe_columns(
        spark, df_stg_carts_products, table_columns
    )
    df_stg_carts_products = df_stg_carts_products.repartition(
        num_partitions, ["person_cart_id", "person_product_id"]
    )

    df_stg_carts_products.write.insertInto(
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
