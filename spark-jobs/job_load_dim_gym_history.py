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

    hash_columns = [
        "api_enabled",
        "blocked",
        "checkin_radius",
        "validation_type",
        "contract_signed_at",
        "disabled_at",
        "disabler_id",
        "disable_b2c",
        "enabled_1",
        "enabled_at",
        "enabled_booking_1",
        "enabler_id",
        "fixed_commission_started_at",
        "force_show_website",
        "gym_network_id",
        "ilimited_min_value",
        "manager_id",
        "max_monthly_value",
        "min_monthly_value",
        "only_token_transactions",
        "open_24h",
        "open_sundays",
        "parent_gym_id",
        "person_id",
        "premium_flag",
        "priority_adjustment_b2b",
        "recorder_id",
        "relationship_manager_id",
        "seller_id",
        "send_welcome_email",
        "source_name",
        "curr_status",
        "token_transactions",
        "token_transactions_started_at",
        "trained_at",
        "trainer_id",
        "unlimited_min_value",
        "webhook_enabled",
    ]

    df_dim_gym_history_old = spark.table(
        f"{jobExec.target_schema}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_gym_history_old = df_dim_gym_history_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_gym_history_old.cache()

    df_dim_gyms = spark.table(f"{jobExec.target_schema}.v_dim_gyms")

    df_gyms = (
        spark.table(f"{jobExec.database_replica_full_history}.gyms")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .select(
            "id",
            "api_enabled",
            "blocked",
            "checkin_radius",
            "contract_signed_at",
            "disabled_at",
            "disabler_id",
            "disable_b2c",
            "enabled_at",
            "enabled_booking_1",
            "enabler_id",
            "fixed_commission_started_at",
            "force_show_website",
            "ilimited_min_value",
            "manager_id",
            "max_monthly_value",
            "only_token_transactions",
            "parent_gym_id",
            "person_id",
            "priority_adjustment_b2b",
            "recorder_id",
            "relationship_manager_id",
            "send_welcome_email",
            "source_name",
            "token_transactions",
            "token_transactions_started_at",
            "trained_at",
            "trainer_id",
            "unlimited_min_value",
            "webhook_enabled",
        )
    )

    df_dim_gym_history_full = (
        df_dim_gyms.join(df_gyms, df_dim_gyms["gym_id"] == df_gyms["id"], "left")
        .select(
            "gym_id",
            "gym_name",
            "gym_network_id",
            "gym_network_name",
            "curr_status",
            "enabled_1",
            "latitude",
            "longitude",
            "gym_status",
            "premium_flag",
            "country_id",
            "first_gym_category",
            "concat_gym_category",
            "open_sundays",
            "open_24h",
            "validation_type",
            "min_monthly_value",
            "monthly_cap",
            "min_monthly_cap",
            "is_booking_enabled",
            "seller_id",
            "full_address",
            "postal_code",
            "neighborhood",
            "sublocality",
            "city",
            "state",
            "state_code",
            "api_enabled",
            "blocked",
            "checkin_radius",
            "contract_signed_at",
            "disabled_at",
            "disabler_id",
            "disable_b2c",
            "enabled_at",
            "enabled_booking_1",
            "enabler_id",
            "fixed_commission_started_at",
            "force_show_website",
            "ilimited_min_value",
            "manager_id",
            "max_monthly_value",
            "only_token_transactions",
            "parent_gym_id",
            "person_id",
            "priority_adjustment_b2b",
            "recorder_id",
            "relationship_manager_id",
            "send_welcome_email",
            "source_name",
            "token_transactions",
            "token_transactions_started_at",
            "trained_at",
            "trainer_id",
            "unlimited_min_value",
            "webhook_enabled",
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .withColumn(
            "reference_date",
            f.lit(jobExec.reference_date).cast(IntegerType()),
        )
        .withColumn("hash_stg", f.hash(*hash_columns))
    )

    df_stg_gym_history_action = (
        df_dim_gym_history_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_gym_history_old.filter(f.col("end_date").isNull()).select(
                "gym_id", "hash_dim"
            ),
            "gym_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_gym_history_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_gym_history_old["hash_dim"].isNotNull())
                & (df_dim_gym_history_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_gym_history_new = (
        df_stg_gym_history_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_gym_history_old.join(
                df_stg_gym_history_action.select("gym_id", "action").filter(
                    f.col("action") == "U"
                ),
                "gym_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(
                    f.col("action").isNull(),
                    df_dim_gym_history_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_gym_history_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNull()),
                    f.lit(jobExec.reference_date).cast(IntegerType()),
                )
                .otherwise(f.lit(jobExec.reference_date).cast(IntegerType())),
            )
            .withColumn(
                "reference_date",
                f.lit(jobExec.reference_date).cast(IntegerType()),
            )
            .drop("action", "hash_dim")
        )
    )

    df_dim_gym_history_new = jobExec.select_dataframe_columns(
        spark, df_dim_gym_history_new, table_columns
    )
    df_dim_gym_history_new = df_dim_gym_history_new.repartition(
        num_partitions, "gym_id"
    )

    df_dim_gym_history_new.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        (spark.table(f"{jobExec.target_schema}.{jobExec.target_table}"))
        .filter(f.col("reference_date") == jobExec.reference_date)
        .count()
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
