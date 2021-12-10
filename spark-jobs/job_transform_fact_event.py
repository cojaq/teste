import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType
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
        "event_name",
        "event_last_modified_date",
        "event_start_date",
        "event_end_date",
        "event_type",
        "event_location",
        "event_is_deleted",
        "event_done_date",
        "event_is_done",
        "event_date",
    ]

    df_event_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.event")
        .withColumn("id", f.trim(f.col("id")))
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_gympass_event = (
        spark.table(f"{jobExec.database_salesforce}.gympass_event")
        .select(
            f.trim(f.col("id")).alias("gympass_event_id"),
            f.col("event_id__c").alias("event_id"),
            "done__c",
        )
        .replace("null", None)
        .replace("", None)
    )

    df_whatid_classification = spark.table(
        f"{jobExec.database_salesforce}.whatid_classification"
    )

    df_fact_event_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)
    df_fact_event_old = df_fact_event_old.withColumn("hash_dim", f.hash(*hash_columns))
    df_fact_event_old.cache()
    df_whatid_classification.cache()

    df_stg_fact_event_delta = (
        df_event_raw.join(
            df_gympass_event,
            df_event_raw["id"] == df_gympass_event["event_id"],
            "inner",
        )
        .join(
            df_whatid_classification.select(
                f.col("objectname").alias("what_id_name"),
                f.col("prefix").alias("what_prefix"),
            ),
            f.substring(df_event_raw["whatid"], 1, 3) == f.col("what_prefix"),
            "left",
        )
        .join(
            df_whatid_classification.select(
                f.col("objectname").alias("who_id_name"),
                f.col("prefix").alias("who_prefix"),
            ),
            f.substring(df_event_raw["whoid"], 1, 3) == f.col("who_prefix"),
            "left",
        )
        .withColumn(
            "event_opportunity_id",
            f.when(
                f.col("what_id_name") == "OPPORTUNITY", df_event_raw["whatid"]
            ).otherwise(f.lit(None).cast(StringType())),
        )
        .withColumn(
            "event_contact_id",
            f.when(
                f.col("what_id_name") == "CONTACT", df_event_raw["whatid"]
            ).otherwise(f.lit(None).cast(StringType())),
        )
        .withColumn(
            "event_lead_id",
            f.when(f.col("who_id_name") == "LEAD", df_event_raw["whoid"]).otherwise(
                f.lit(None).cast(StringType())
            ),
        )
        .withColumn(
            "event_object_type_related",
            f.coalesce(f.col("what_id_name"), f.col("who_id_name")),
        )
        .withColumn(
            "event_is_done",
            f.when(
                f.col("done__c").isNotNull(),
                f.when(f.upper(f.col("done__c")) == "YES", f.lit(True)).otherwise(
                    False
                ),
            ).otherwise(False),
        )
        .withColumn(
            "event_done_date",
            f.when(
                f.col("done__c").isNotNull(),
                f.when(
                    f.upper(f.col("done__c")) == "YES",
                    f.date_format(df_event_raw["lastmodifieddate"], "yyyyMMdd").cast(
                        IntegerType()
                    ),
                ).otherwise(None),
            ),
        )
        .withColumn(
            "event_date",
            f.date_format(df_event_raw["startdatetime"], "yyyyMMdd").cast(
                IntegerType()
            ),
        )
        .select(
            df_gympass_event["gympass_event_id"].alias("event_id"),
            df_event_raw["accountid"].alias("event_account_id"),
            f.col("event_opportunity_id"),
            f.col("event_contact_id"),
            f.col("event_lead_id"),
            f.col("event_object_type_related"),
            df_event_raw["type"].alias("event_type"),
            df_event_raw["subject"].alias("event_name"),
            df_event_raw["location"].alias("event_location"),
            df_event_raw["createddate"].alias("event_created_date"),
            df_event_raw["lastmodifieddate"].alias("event_last_modified_date"),
            df_event_raw["startdatetime"].alias("event_start_date"),
            df_event_raw["enddatetime"].alias("event_end_date"),
            f.col("event_is_done"),
            f.col("event_done_date"),
            f.col("event_date"),
            df_event_raw["isdeleted"].alias("event_is_deleted"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_stg_fact_event_action = (
        df_stg_fact_event_delta.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_fact_event_old.select("event_id", "hash_dim"),
            "event_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_fact_event_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_fact_event_old["hash_dim"].isNotNull())
                & (df_fact_event_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_fact_event_new = (
        df_stg_fact_event_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_fact_event_old.join(
                df_stg_fact_event_action.select("event_id", "action"),
                "event_id",
                "left_anti",
            )
            .withColumn(
                "reference_date",
                f.lit(jobExec.reference_date).cast(IntegerType()),
            )
            .drop("action", "hash_dim")
        )
    )

    df_fact_event_new = jobExec.select_dataframe_columns(
        spark, df_fact_event_new, table_columns
    )
    df_fact_event_new = df_fact_event_new.repartition(num_partitions, "event_id")

    df_fact_event_new.write.insertInto(
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
