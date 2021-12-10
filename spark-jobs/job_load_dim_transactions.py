import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    ShortType,
    StringType,
    TimestampType,
)
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

    # v_dim_transactions column list to be used in select
    dim_transaction_fields = [
        "data_source",
        "person_cart_id",
        "person_id",
        "purchase_person_id",
        "company_id",
        "date",
        "date_hour",
        "refund_date",
        "action",
        "gym_id",
        "country_id",
        "payment_method",
        "payment_type",
        "enrollment_type",
        "gym_product_value",
        "token_group_type",
        "active_plan_value",
        "sales_value",
        "revenue_value",
        "gym_expenses_value",
        "refund_value",
        "valid_start_day",
        "valid_end_day",
        "valid_end_day_adjusted",
        "valid_days_curr_month",
        "valid_days_next_month",
        "total_price",
        "partner_visit_action",
        "manual_inputs_description",
        "original_adjustment_date",
        "adjustment_comments",
    ]

    df_stg_carts_products = spark.table(f"{jobExec.database_work}.stg_carts_products")

    df_stg_gym_visits = spark.table(f"{jobExec.database_work}.stg_gym_visits")

    df_stg_carts_products = (
        df_stg_carts_products.withColumn(
            "valid_end_day_adjusted_dt",
            f.when(
                f.col("valid_end_day")
                < f.add_months(f.date_sub(f.col("valid_start_day"), 1), 1),
                f.col("valid_end_day"),
            ).otherwise(f.add_months(f.date_sub(f.col("valid_start_day"), 1), 1)),
        )
        .withColumn(
            "valid_days_curr_month",
            f.datediff(
                f.last_day(f.col("valid_start_day")),
                f.date_sub(f.col("valid_start_day"), 1),
            ),
        )
        .withColumn(
            "valid_days_next_month",
            f.when(
                f.trunc(f.col("valid_start_day"), "month")
                != f.trunc(f.col("valid_end_day_adjusted_dt"), "month"),
                f.datediff(
                    f.date_add(f.col("valid_end_day_adjusted_dt"), 1),
                    f.trunc(f.col("valid_end_day_adjusted_dt"), "month"),
                ),
            ).otherwise(f.lit(0)),
        )
        .withColumn(
            "valid_end_day_adjusted",
            f.date_format(f.col("valid_end_day_adjusted_dt"), "yyyyMMdd").cast(
                IntegerType()
            ),
        )
        .withColumn(
            "valid_start_day",
            f.date_format(f.col("valid_start_day"), "yyyyMMdd").cast(IntegerType()),
        )
        .withColumn(
            "valid_end_day",
            f.date_format(f.col("valid_end_day"), "yyyyMMdd").cast(IntegerType()),
        )
        .drop("valid_end_day_adjusted_dt")
    )

    df_stg_carts_products.cache()

    # PURCHASERS
    df_dim_transactions_1 = (
        df_stg_carts_products.filter(
            df_stg_carts_products["report_id"].isin(25, 16) == False
        )
        .filter(df_stg_carts_products["paid_at_converted"].isNotNull())
        .withColumn("data_source", f.lit("automatic"))
        .withColumn(
            "date",
            (
                f.date_format(df_stg_carts_products["paid_at_converted"], "yyyyMMdd")
            ).cast(IntegerType()),
        )
        .withColumn(
            "date_hour",
            (f.date_format(df_stg_carts_products["paid_at_converted"], "H")).cast(
                ShortType()
            ),
        )
        .withColumn("refund_date", f.lit(None).cast(IntegerType()))
        .withColumn(
            "action",
            f.when(
                df_stg_carts_products["product_id"] == 19,
                f.lit("cancellation"),
            )
            .when(
                df_stg_carts_products["product_id"] == 30,
                f.lit("user_late_cancel"),
            )
            .when(
                df_stg_carts_products["product_id"] == 31,
                f.lit("user_no_show"),
            )
            .when(
                df_stg_carts_products["report_id"].isin(1, 17, 18, 20, 29),
                f.lit("purchase"),
            )
            .when(
                df_stg_carts_products["product_id"] == 22,
                f.lit("others - gift credits"),
            )
            .otherwise(f.lit("others")),
        )
        .withColumn(
            "gym_id",
            f.when(
                df_stg_carts_products["report_id"] == 1,
                df_stg_carts_products["gym_id"],
            ).otherwise(None),
        )
        .withColumnRenamed("payment_method_type", "payment_method")
        .withColumn(
            "payment_type",
            f.when(
                (df_stg_carts_products["report_id"].isin(16, 25, 27, 28) == False)
                | (df_stg_carts_products["product_id"] != 19),
                f.when(
                    df_stg_carts_products["has_copay_discount"] == 1,
                    f.lit("free"),
                ).otherwise(f.lit("paid")),
            ).otherwise(None),
        )
        .withColumn(
            "enrollment_type",
            f.when(
                (df_stg_carts_products["report_id"].isin(16, 25, 27, 28))
                | (df_stg_carts_products["product_id"] == 19),
                f.lit(None),
            )
            .when(df_stg_carts_products["product_id"].isin(30, 31), f.lit(None))
            .when(
                df_stg_carts_products["report_id"].isin(17, 18, 20, 29),
                f.lit("unlimited"),
            )
            .otherwise("daypass"),
        )
        .withColumnRenamed("monthly_value", "gym_product_value")
        .withColumn("token_group_type", f.lit(None).cast(StringType()))
        .withColumnRenamed("plan_value", "active_plan_value")
        .withColumn(
            "sales_value",
            f.when(
                df_stg_carts_products["company_type"].isin(20, 30, 40),
                (
                    df_stg_carts_products["total_revenue"]
                    - df_stg_carts_products["subsidy_value"]
                ).cast(DecimalType(12, 2)),
            )
            .when(
                df_stg_carts_products["company_type"] == 42,
                df_stg_carts_products["subsidy_to_gympass_value"],
            )
            .when(
                df_stg_carts_products["company_type"].isin(45, 50),
                df_stg_carts_products["total_revenue"],
            )
            .when(
                df_stg_carts_products["company_type"] == 14,
                df_stg_carts_products["total_revenue"],
            )
            .otherwise(
                (
                    df_stg_carts_products["subsidy_value"]
                    + df_stg_carts_products["subsidy_to_gympass_value"]
                    + df_stg_carts_products["taxable_total_price"]
                ).cast(DecimalType(12, 2))
            ),
        )
        .withColumn(
            "revenue_value",
            f.when(
                df_stg_carts_products["company_type"].isin(20, 30, 40),
                (
                    df_stg_carts_products["total_revenue"]
                    - df_stg_carts_products["subsidy_value"]
                ).cast(DecimalType(12, 2)),
            )
            .when(
                df_stg_carts_products["company_type"] == 42,
                df_stg_carts_products["subsidy_to_gympass_value"],
            )
            .when(
                df_stg_carts_products["company_type"].isin(45, 50),
                df_stg_carts_products["total_revenue"],
            )
            .when(
                df_stg_carts_products["company_type"] == 14,
                df_stg_carts_products["total_revenue"],
            )
            .otherwise(
                (
                    df_stg_carts_products["subsidy_value"]
                    + df_stg_carts_products["subsidy_to_gympass_value"]
                    + df_stg_carts_products["taxable_total_price"]
                ).cast(DecimalType(12, 2))
            ),
        )
        .withColumn("gym_expenses_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn("refund_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn("partner_visit_action", f.lit(None).cast(StringType()))
        .withColumn("manual_inputs_description", f.lit(None).cast(StringType()))
        .withColumn("original_adjustment_date", f.lit(None).cast(IntegerType()))
        .withColumn("adjustment_comments", f.lit(None).cast(StringType()))
        .groupBy("person_cart_id", "date", "date_hour", "data_source", "action")
        .agg(
            f.max("person_id").alias("person_id"),
            f.max("purchase_person_id").alias("purchase_person_id"),
            f.max("company_id").alias("company_id"),
            f.max("refund_date").alias("refund_date"),
            f.max("gym_id").alias("gym_id"),
            f.max("country_id").alias("country_id"),
            f.max("payment_method").alias("payment_method"),
            f.max("payment_type").alias("payment_type"),
            f.max("enrollment_type").alias("enrollment_type"),
            f.max("gym_product_value").alias("gym_product_value"),
            f.max("token_group_type").alias("token_group_type"),
            f.max("active_plan_value").alias("active_plan_value"),
            f.max("sales_value").alias("sales_value"),
            f.max("revenue_value").alias("revenue_value"),
            f.max("gym_expenses_value").alias("gym_expenses_value"),
            f.max("refund_value").alias("refund_value"),
            f.max("valid_start_day").alias("valid_start_day"),
            f.max("valid_end_day").alias("valid_end_day"),
            f.max("valid_end_day_adjusted").alias("valid_end_day_adjusted"),
            f.max("valid_days_curr_month").alias("valid_days_curr_month"),
            f.max("valid_days_next_month").alias("valid_days_next_month"),
            f.max("total_price").alias("total_price"),
            f.max("partner_visit_action").alias("partner_visit_action"),
            f.max("manual_inputs_description").alias("manual_inputs_description"),
            f.max("original_adjustment_date").alias("original_adjustment_date"),
            f.max("adjustment_comments").alias("adjustment_comments"),
        )
        .select(dim_transaction_fields)
    )

    # LICENSE FEE
    df_dim_transactions_company_fee = (
        df_stg_carts_products.filter(df_stg_carts_products["subsidy_value"] > 0)
        .withColumn("data_source", f.lit("automatic"))
        .withColumn(
            "person_cart_id",
            f.when(
                df_stg_carts_products["subsidy_value"] > 0,
                df_stg_carts_products["person_cart_id"],
            ).otherwise(None),
        )
        .withColumn(
            "date",
            (
                f.date_format(df_stg_carts_products["paid_at_converted"], "yyyyMMdd")
            ).cast(IntegerType()),
        )
        .withColumn(
            "date_agg_month",
            (f.date_format(df_stg_carts_products["paid_at_converted"], "yyyyMM")).cast(
                IntegerType()
            ),
        )
        .withColumn("date_hour", f.lit(None).cast(ShortType()))
        .withColumn(
            "country_id",
            f.when(
                df_stg_carts_products["subsidy_value"] > 0,
                df_stg_carts_products["country_id"],
            ).otherwise(None),
        )
        .groupBy("data_source", "date_agg_month", "company_id")
        .agg(
            f.max("person_cart_id").alias("person_cart_id"),
            f.max("date").alias("date"),
            f.max("date_hour").alias("date_hour"),
            f.max("country_id").alias("country_id"),
            f.sum("subsidy_value").cast(DecimalType(12, 2)).alias("sales_value"),
            f.sum("subsidy_value").cast(DecimalType(12, 2)).alias("revenue_value"),
            f.max("valid_start_day").alias("valid_start_day"),
            f.max("valid_end_day").alias("valid_end_day"),
            f.max("valid_end_day_adjusted").alias("valid_end_day_adjusted"),
            f.max("valid_days_curr_month").alias("valid_days_curr_month"),
            f.max("valid_days_next_month").alias("valid_days_next_month"),
            f.max("total_price").alias("total_price"),
        )
        .withColumn("person_id", f.lit(None).cast(IntegerType()))
        .withColumn("purchase_person_id", f.lit(None).cast(IntegerType()))
        .withColumn("refund_date", f.lit(None).cast(IntegerType()))
        .withColumn("action", f.lit("company_fee"))
        .withColumn("gym_id", f.lit(None).cast(IntegerType()))
        .withColumn("payment_method", f.lit(None).cast(StringType()))
        .withColumn("payment_type", f.lit(None).cast(StringType()))
        .withColumn("enrollment_type", f.lit(None).cast(StringType()))
        .withColumn("gym_product_value", f.lit(None).cast(DecimalType(12, 2)))
        .withColumn("token_group_type", f.lit(None).cast(StringType()))
        .withColumn("active_plan_value", f.lit(None).cast(DecimalType(12, 2)))
        .withColumn("gym_expenses_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn("refund_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn("partner_visit_action", f.lit(None).cast(StringType()))
        .withColumn("manual_inputs_description", f.lit(None).cast(StringType()))
        .withColumn("original_adjustment_date", f.lit(None).cast(IntegerType()))
        .withColumn("adjustment_comments", f.lit(None).cast(StringType()))
        .select(dim_transaction_fields)
    )

    # REFUNDS
    df_dim_transactions_3 = (
        df_stg_carts_products.filter(
            df_stg_carts_products["refunded_at_converted"].isNotNull()
        )
        .withColumn("data_source", f.lit("automatic"))
        .withColumn(
            "date",
            (
                f.date_format(
                    df_stg_carts_products["refunded_at_converted"], "yyyyMMdd"
                )
            ).cast(IntegerType()),
        )
        .withColumn("date_hour", f.lit(None).cast(ShortType()))
        .withColumn(
            "refund_date",
            (
                f.date_format(
                    df_stg_carts_products["refunded_at_converted"], "yyyyMMdd"
                )
            ).cast(IntegerType()),
        )
        .withColumn(
            "action",
            f.when(
                df_stg_carts_products["report_id"] == 25,
                f.lit("refund_company_fee").cast(StringType()),
            )
            .when(
                df_stg_carts_products["product_id"] == 30,
                f.lit("refund_user_late_cancel"),
            )
            .when(
                df_stg_carts_products["product_id"] == 31,
                f.lit("refund_user_no_show"),
            )
            .when(
                df_stg_carts_products["report_id"].isin(1, 17, 18, 20, 29),
                f.lit("refund_purchase").cast(StringType()),
            )
            .otherwise(f.lit("refund_others").cast(StringType())),
        )
        .withColumn(
            "gym_id",
            f.when(
                df_stg_carts_products["report_id"] == 1,
                df_stg_carts_products["gym_id"],
            ).otherwise(None),
        )
        .withColumnRenamed("payment_method_type", "payment_method")
        .withColumn(
            "payment_type",
            f.when(
                (df_stg_carts_products["report_id"].isin(16, 25, 27, 28) == False)
                | (df_stg_carts_products["product_id"] != 19),
                f.when(
                    df_stg_carts_products["has_copay_discount"] == 1,
                    f.lit("free"),
                ).otherwise(f.lit("paid")),
            ).otherwise(None),
        )
        .withColumn(
            "enrollment_type",
            f.when(
                (df_stg_carts_products["report_id"].isin(16, 25, 27, 28))
                | (df_stg_carts_products["product_id"] == 19),
                f.lit(None),
            )
            .when(df_stg_carts_products["product_id"].isin(30, 31), f.lit(None))
            .when(
                df_stg_carts_products["report_id"].isin(17, 18, 20, 29),
                f.lit("unlimited"),
            )
            .otherwise("daypass"),
        )
        .withColumnRenamed("monthly_value", "gym_product_value")
        .withColumn("token_group_type", f.lit(None).cast(StringType()))
        .withColumn(
            "active_plan_value",
            f.when(
                (f.coalesce(df_stg_carts_products["plan_value"], f.lit(0)) == 0)
                & (df_stg_carts_products["report_id"].isin(17, 18)),
                df_stg_carts_products["unit_price"],
            ).otherwise(df_stg_carts_products["plan_value"]),
        )
        .withColumn("sales_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn(
            "revenue_value",
            f.when(
                df_stg_carts_products["total_refunded_value"].isNotNull(),
                (df_stg_carts_products["total_refunded_value"] * -1).cast(
                    DecimalType(12, 2)
                ),
            ).otherwise(f.lit(None).cast(DecimalType(12, 2))),
        )
        .withColumn("gym_expenses_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumnRenamed("total_refunded_value", "refund_value")
        .withColumn("partner_visit_action", f.lit(None).cast(StringType()))
        .withColumn("manual_inputs_description", f.lit(None).cast(StringType()))
        .withColumn("original_adjustment_date", f.lit(None).cast(IntegerType()))
        .withColumn("adjustment_comments", f.lit(None).cast(StringType()))
        .select(dim_transaction_fields)
        .distinct()
    )

    df_stg_gym_visits = df_stg_gym_visits.drop("person_cart_id")

    # GYM VISITS
    df_dim_transactions_gym_visits = (
        df_stg_gym_visits.withColumn("data_source", f.lit("automatic"))
        .withColumnRenamed("parent_plan_person_cart_id", "person_cart_id")
        .withColumn("purchase_person_id", f.lit(None).cast(IntegerType()))
        .withColumn(
            "date",
            (df_stg_gym_visits["considered_hour"] / 100).cast(IntegerType()),
        )
        .withColumn(
            "date_hour",
            (df_stg_gym_visits["considered_hour"] % 100).cast(IntegerType()),
        )
        .withColumn(
            "refund_date",
            (
                f.date_format(df_stg_gym_visits["refunded_at_converted"], "yyyyMMdd")
            ).cast(IntegerType()),
        )
        .withColumn(
            "action",
            f.when(f.col("report_id").isin(27, 33), f.lit("gym_bonus"))
            .when(
                f.col("person_transactions_product_id") == 30,
                f.lit("gym_late_cancel"),
            )
            .when(
                f.col("person_transactions_product_id") == 31,
                f.lit("gym_no_show"),
            )
            .when(f.col("retroactive_validation") == 1, f.lit("gym_visit_retro"))
            .otherwise(f.lit("gym_visit")),
        )
        .withColumnRenamed("payment_method_type", "payment_method")
        .withColumn("active_plan_value", f.col("plan_value"))
        .withColumn(
            "payment_type",
            f.when(
                (df_stg_gym_visits["report_id"].isin(16, 25, 27, 28) == False)
                | (df_stg_gym_visits["product_id"] != 19),
                f.when(
                    df_stg_gym_visits["has_copay_discount"] == 1,
                    f.lit("free"),
                ).otherwise(f.lit("paid")),
            ).otherwise(None),
        )
        .withColumn(
            "enrollment_type",
            f.when(
                (df_stg_gym_visits["report_id"].isin(16, 25, 27, 28))
                | (df_stg_gym_visits["product_id"] == 19),
                f.lit(None),
            )
            .when(
                df_stg_gym_visits["person_transactions_product_id"].isin(30, 31),
                f.lit(None),
            )
            .when(
                df_stg_gym_visits["report_id"].isin(17, 18, 20, 29),
                f.lit("unlimited"),
            )
            .otherwise("daypass"),
        )
        .withColumn("sales_value", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn("revenue_value", f.col("gym_commission") * -1)
        .withColumn("gym_expenses_value", f.col("gym_commission"))
        .withColumnRenamed("total_refunded_value", "refund_value")
        .withColumn("valid_start_day", f.lit(0))
        .withColumn("valid_end_day", f.lit(0))
        .withColumn("valid_end_day_adjusted", f.lit(0))
        .withColumn("valid_days_curr_month", f.lit(0))
        .withColumn("valid_days_next_month", f.lit(0))
        .withColumn("total_price", f.lit(0).cast(DecimalType(12, 2)))
        .withColumn(
            "partner_visit_action",
            f.when(f.col("report_id") == 27, f.lit("gym_bonus"))
            .when(f.col("report_id") == 33, f.lit("gym_bonus_second_attempt"))
            .when(
                f.col("person_transactions_product_id") == 32,
                f.lit("virtual_class"),
            )
            .when(
                f.col("person_transactions_product_id") == 30,
                f.lit("in_person_class"),
            )
            .when(
                f.col("person_transactions_product_id") == 31,
                f.lit("in_person_class"),
            )
            .when(f.col("retroactive_validation") == 1, f.lit("in_person_class"))
            .otherwise(f.lit("in_person_class")),
        )
        .withColumn("manual_inputs_description", f.lit(None).cast(StringType()))
        .withColumn("original_adjustment_date", f.lit(None).cast(IntegerType()))
        .withColumn("adjustment_comments", f.lit(None).cast(StringType()))
        .select(dim_transaction_fields)
    )

    df_dim_transactions_actions = (
        df_dim_transactions_1.union(df_dim_transactions_company_fee)
        .union(df_dim_transactions_3)
        .union(df_dim_transactions_gym_visits)
    )

    # ADJUSTMENTS FOR NEGATIVE PURCHASES
    df_dim_transactions_final = (
        df_dim_transactions_actions.filter(
            df_dim_transactions_actions["sales_value"] < 0
        )
        .withColumn(
            "data_source",
            f.lit("manual - negative purchase").cast(StringType()),
        )
        .withColumn(
            "sales_value",
            (df_dim_transactions_actions["sales_value"] * -1).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "revenue_value",
            (df_dim_transactions_actions["revenue_value"] * -1).cast(
                DecimalType(12, 2)
            ),
        )
        .withColumn(
            "gym_expenses_value",
            (df_dim_transactions_actions["gym_expenses_value"] * -1).cast(
                DecimalType(12, 2)
            ),
        )
        .withColumn(
            "refund_value",
            (df_dim_transactions_actions["refund_value"] * -1).cast(DecimalType(12, 2)),
        )
        .withColumn("valid_start_day", f.lit(0))
        .withColumn("valid_end_day", f.lit(0))
        .withColumn("valid_end_day_adjusted", f.lit(0))
        .withColumn("valid_days_curr_month", f.lit(0))
        .withColumn("valid_days_next_month", f.lit(0))
        .withColumn("total_price", f.lit(0).cast(DecimalType(12, 2)))
        .select(dim_transaction_fields)
        .union(df_dim_transactions_actions)
    )

    df_dim_transactions_final = jobExec.select_dataframe_columns(
        spark, df_dim_transactions_final, table_columns
    )
    df_dim_transactions_final = df_dim_transactions_final.repartition(
        num_partitions, "person_cart_id"
    )

    df_dim_transactions_final.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()

    if jobExec.totalLines > 0:
        table_location = dataframe_utils.returnHiveTableLocation(
            spark,
            jobExec.target_schema,
            jobExec.target_table,
            False,
            jobExec.reference_date,
        )

        delete_statement = f"DELETE FROM {jobExec.database_public}.{jobExec.target_table} WHERE data_source != 'manual'"
        jobExec.redshift.executeStatement(delete_statement, "delete")

        jobExec.redshift.LoadS3toRedshift(
            table_location, jobExec.database_public, jobExec.target_table
        )
    else:
        jobExec.logger.warning("Target table is empty")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
