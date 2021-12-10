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
        "account_parent_account_id",
        "account_billing_country",
        "account_billing_city",
        "account_number_of_employees",
        "account_owner_user_id",
        "account_source",
        "account_crm_id",
        "account_has_opportunity",
        "account_is_converted",
        "account_gympass_status",
        "account_gympass_blacklist",
        "account_size",
        "account_bdr_user_id",
        "account_wish_list",
        "account_gym_subtype",
    ]

    df_account_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.account")
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_record_type_description = spark.table(
        f"{jobExec.database_salesforce}.record_type_description"
    )

    df_dim_account_old = spark.table(
        f"{jobExec.database_dm_salesforce}.dim_account"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_account_old = df_dim_account_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_account_old.cache()

    df_stg_account_full = (
        df_account_raw.join(
            df_record_type_description,
            df_account_raw["recordtypeid"] == df_record_type_description["id"],
            "inner",
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .select(
            df_account_raw["id"].alias("account_id"),
            df_account_raw["name"].alias("account_name"),
            df_account_raw["type"].alias("account_type"),
            df_record_type_description["name"].alias("account_record_type_name"),
            df_account_raw["parentid"].alias("account_parent_account_id"),
            df_account_raw["billingcountry"].alias("account_billing_country"),
            df_account_raw["billingcity"].alias("account_billing_city"),
            df_account_raw["industry"].alias("account_industry"),
            df_account_raw["numberofemployees"].alias("account_number_of_employees"),
            df_account_raw["currencyisocode"].alias("account_currency_iso_code"),
            df_account_raw["ownerid"].alias("account_owner_user_id"),
            df_account_raw["accountsource"].alias("account_source"),
            df_account_raw["id_gympass_crm__c"].alias("account_crm_id"),
            df_account_raw["razao_social__c"].alias("account_business_name"),
            df_account_raw["conta_tem_oportunidade__c"].alias(
                "account_has_opportunity"
            ),
            df_account_raw["convertido__c"].alias("account_is_converted"),
            df_account_raw["gp_status__c"].alias("account_gympass_status"),
            df_account_raw["gp_blacklist__c"].alias("account_gympass_blacklist"),
            df_account_raw["size__c"].alias("account_size"),
            df_account_raw["bdr__c"].alias("account_bdr_user_id"),
            df_account_raw["wishlist__c"].alias("account_wish_list"),
            df_account_raw["gym_subtype__c"].alias("account_gym_subtype"),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_stg_account_action = (
        df_stg_account_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_account_old.filter(f.col("end_date").isNull()).select(
                "account_id", "hash_dim"
            ),
            "account_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_account_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_account_old["hash_dim"].isNotNull())
                & (df_dim_account_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_account_new = (
        df_stg_account_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_account_old.join(
                df_stg_account_action.select("account_id", "action").filter(
                    f.col("action") == "U"
                ),
                "account_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(f.col("action").isNull(), df_dim_account_old["end_date"])
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_account_old["end_date"],
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

    df_dim_account_new = jobExec.select_dataframe_columns(
        spark, df_dim_account_new, table_columns
    )
    df_dim_account_new = df_dim_account_new.repartition(num_partitions, "account_id")

    df_dim_account_new.repartition(num_partitions).write.insertInto(
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
