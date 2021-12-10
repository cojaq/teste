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
        "lead_is_deleted",
        "lead_first_name",
        "lead_last_name",
        "lead_company",
        "lead_city",
        "lead_state",
        "lead_state_code",
        "lead_country",
        "lead_status",
        "owner_user_id",
        "has_opted_out_of_email",
        "lead_is_converted",
        "lead_converted_date",
        "converted_account_id",
        "converted_contact_id",
        "converted_opportunity_id",
        "lead_job_position",
        "lead_department",
        "lead_sub",
    ]

    df_lead_raw = spark.table(f"{jobExec.database_salesforce_raw}.lead")
    df_lead_raw = (
        df_lead_raw.filter(f.col("reference_date") == jobExec.reference_date)
        .filter(f.lower(f.col("id")) != "id")
        .replace("null", None)
        .replace("", None)
    )

    df_record_type_description = spark.table(
        f"{jobExec.database_salesforce}.record_type_description"
    )

    df_dim_salesforce_lead_old = spark.table(
        f"{jobExec.database_dm_salesforce}.dim_lead"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_salesforce_lead_old = df_dim_salesforce_lead_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_salesforce_lead_old.cache()

    df_stg_lead_full = (
        df_lead_raw.join(
            df_record_type_description,
            df_lead_raw["recordtypeid"] == df_record_type_description["id"],
            "left",
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .select(
            df_lead_raw["id"].alias("lead_id"),
            df_lead_raw["isdeleted"].alias("lead_is_deleted"),
            df_lead_raw["firstname"].alias("lead_first_name"),
            df_lead_raw["lastname"].alias("lead_last_name"),
            df_record_type_description["name"].alias("lead_record_type_name"),
            df_lead_raw["title"].alias("lead_title"),
            df_lead_raw["company"].alias("lead_company"),
            df_lead_raw["city"].alias("lead_city"),
            df_lead_raw["state"].alias("lead_state"),
            df_lead_raw["statecode"].alias("lead_state_code"),
            df_lead_raw["country"].alias("lead_country"),
            df_lead_raw["countrycode"].alias("lead_country_code"),
            df_lead_raw["status"].alias("lead_status"),
            df_lead_raw["industry"].alias("lead_industry"),
            df_lead_raw["rating"].alias("lead_rating"),
            df_lead_raw["numberofemployees"].alias("number_of_employees"),
            df_lead_raw["ownerid"].alias("owner_user_id"),
            df_lead_raw["hasoptedoutofemail"].alias("has_opted_out_of_email"),
            df_lead_raw["isconverted"].alias("lead_is_converted"),
            df_lead_raw["converteddate"].alias("lead_converted_date"),
            df_lead_raw["convertedaccountid"].alias("converted_account_id"),
            df_lead_raw["convertedcontactid"].alias("converted_contact_id"),
            df_lead_raw["convertedopportunityid"].alias("converted_opportunity_id"),
            df_lead_raw["isunreadbyowner"].alias("is_unread_by_ower"),
            df_lead_raw["cargo_do_contato__c"].alias("lead_job_position"),
            df_lead_raw["department__c"].alias("lead_department"),
            df_lead_raw["lead_sub__c"].alias("lead_sub"),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
        .withColumn("hash_stg", f.hash(*hash_columns))
    )

    df_stg_lead_action = (
        df_stg_lead_full.join(
            df_dim_salesforce_lead_old.filter(f.col("end_date").isNull()).select(
                "lead_id", "hash_dim"
            ),
            "lead_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_salesforce_lead_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_salesforce_lead_old["hash_dim"].isNotNull())
                & (df_dim_salesforce_lead_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_salesforce_lead_new = (
        df_stg_lead_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_salesforce_lead_old.join(
                df_stg_lead_action.select("lead_id", "action").filter(
                    f.col("action") == "U"
                ),
                "lead_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(
                    f.col("action").isNull(),
                    df_dim_salesforce_lead_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_salesforce_lead_old["end_date"],
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

    df_dim_salesforce_lead_new = jobExec.select_dataframe_columns(
        spark, df_dim_salesforce_lead_new, table_columns
    )
    df_dim_salesforce_lead_new.repartition(num_partitions).write.insertInto(
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
