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

    hash_columns = ["event_user_is_deleted"]

    df_gympass_leader_association_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.gympass_leader_association")
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_dim_event_user_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_event_user_old = df_dim_event_user_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_event_user_old.cache()

    df_dim_event_user_full = (
        df_gympass_leader_association_raw.withColumn(
            "user_is_leader",
            f.when(f.upper(f.col("leader__c")) == "YES", f.lit(True)).otherwise(
                f.lit(False)
            ),
        )
        .withColumn(
            "user_is_relationship",
            f.when(f.upper(f.col("relationship__c")) == "YES", f.lit(True)).otherwise(
                f.lit(False)
            ),
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .select(
            df_gympass_leader_association_raw["id"].alias("event_user_id"),
            df_gympass_leader_association_raw["gympass_event__c"].alias("event_id"),
            df_gympass_leader_association_raw["gympass_leader__c"].alias("user_id"),
            f.col("user_is_leader"),
            f.col("user_is_relationship"),
            df_gympass_leader_association_raw["type__c"].alias("event_user_type"),
            df_gympass_leader_association_raw["isdeleted"].alias(
                "event_user_is_deleted"
            ),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_dim_event_user_action = (
        df_dim_event_user_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_event_user_old.filter(f.col("end_date").isNull()).select(
                "event_user_id", "hash_dim"
            ),
            "event_user_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_event_user_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_event_user_old["hash_dim"].isNotNull())
                & (df_dim_event_user_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_event_user_new = (
        df_dim_event_user_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_event_user_old.join(
                df_dim_event_user_action.select("event_user_id", "action").filter(
                    f.col("action") == "U"
                ),
                "event_user_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(
                    f.col("action").isNull(),
                    df_dim_event_user_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_event_user_old["end_date"],
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

    df_dim_event_user_new = jobExec.select_dataframe_columns(
        spark, df_dim_event_user_new, table_columns
    )
    df_dim_event_user_new = df_dim_event_user_new.repartition(
        num_partitions, "event_user_id"
    )

    df_dim_event_user_new.write.insertInto(
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
