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
    jobExec.target_schema if jobExec.target_schema else jobExec.database_dm_salesforce
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    hash_columns = [
        "opportunity_stage_name",
        "opportunity_amount",
        "opportunity_expected_revenue",
        "opportunity_close_date",
        "opportunity_probability",
        "opportunity_forecast_category",
        "opportunity_currency_iso_code",
        "opportunity_is_deleted",
    ]

    df_opportunity_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.opportunity")
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_account = spark.table(f"{jobExec.database_salesforce}.account")

    df_record_type_description = spark.table(
        f"{jobExec.database_salesforce}.record_type_description"
    )

    df_dim_opportunity_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_opportunity_old = df_dim_opportunity_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_opportunity_old.cache()

    df_stg_opportunity_full = (
        df_opportunity_raw.join(
            df_account,
            df_opportunity_raw["accountid"] == df_account["id"],
            "left",
        )
        .join(
            df_record_type_description,
            df_opportunity_raw["recordtypeid"] == df_record_type_description["id"],
            "inner",
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .withColumn(
            "opportunity_close_date",
            f.to_date(df_opportunity_raw["closedate"]),
        )
        .withColumn(
            "opportunity_launch_date",
            f.to_date(df_opportunity_raw["data_do_lancamento__c"]),
        )
        .select(
            df_opportunity_raw["id"].alias("opportunity_id"),
            df_opportunity_raw["name"].alias("opportunity_name"),
            df_opportunity_raw["accountid"].alias("opportunity_account_id"),
            df_account["name"].alias("opportunity_account_name"),
            df_record_type_description["name"].alias("opportunity_record_type_name"),
            df_opportunity_raw["stagename"].alias("opportunity_stage_name"),
            df_opportunity_raw["forecastcategory"].alias(
                "opportunity_forecast_category"
            ),
            df_opportunity_raw["amount"].alias("opportunity_amount"),
            df_opportunity_raw["probability"].alias("opportunity_probability"),
            df_opportunity_raw["expectedrevenue"].alias("opportunity_expected_revenue"),
            df_opportunity_raw["totalopportunityquantity"].alias(
                "opportunity_total_quantity"
            ),
            f.col("opportunity_close_date"),
            df_opportunity_raw["isclosed"].alias("opportunity_is_closed"),
            df_opportunity_raw["type"].alias("opportunity_type"),
            df_opportunity_raw["iswon"].alias("opportunity_is_won"),
            df_opportunity_raw["loss_reason__c"].alias("opportunity_currency_iso_code"),
            df_opportunity_raw["currencyisocode"].alias("opportunity_loss_reason"),
            df_opportunity_raw["data_do_lancamento__c"].alias(
                "opportunity_launch_date"
            ),
            df_opportunity_raw["reseller_del__c"].alias(
                "opportunity_reseller_account_id"
            ),
            df_opportunity_raw["ownerid"].alias("opportunity_owner_user_id"),
            df_opportunity_raw["bdr__c"].alias("opportunity_bdr_user_id"),
            df_opportunity_raw["responsavel_gympass_relacionamento__c"].alias(
                "opportunity_relationship_responsible_user_id"
            ),
            df_opportunity_raw["country_manager_approval__c"].alias(
                "opportunity_is_country_manager_approved"
            ),
            df_opportunity_raw["isdeleted"].alias("opportunity_is_deleted"),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_stg_opportunity_action = (
        df_stg_opportunity_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_opportunity_old.filter(f.col("end_date").isNull()).select(
                "opportunity_id", "hash_dim"
            ),
            "opportunity_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_opportunity_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_opportunity_old["hash_dim"].isNotNull())
                & (df_dim_opportunity_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_opportunity_new = (
        df_stg_opportunity_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_opportunity_old.join(
                df_stg_opportunity_action.select("opportunity_id", "action").filter(
                    f.col("action") == "U"
                ),
                "opportunity_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(
                    f.col("action").isNull(),
                    df_dim_opportunity_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_opportunity_old["end_date"],
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

    df_dim_opportunity_new = jobExec.select_dataframe_columns(
        spark, df_dim_opportunity_new, table_columns
    )
    df_dim_opportunity_new = df_dim_opportunity_new.repartition(
        num_partitions, "opportunity_id"
    )

    df_dim_opportunity_new.write.insertInto(
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
