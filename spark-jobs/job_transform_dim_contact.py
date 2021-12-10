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
        "contact_first_name",
        "contact_last_name",
        "contact_city",
        "contact_state",
        "contact_country",
        "contact_job_title",
        "contact_company_industry",
        "contact_status",
        "contact_is_deleted",
    ]

    df_contact_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.contact")
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_dim_contact_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_contact_old = df_dim_contact_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_contact_old.cache()

    df_dim_contact_full = (
        df_contact_raw.withColumn(
            "contact_is_decision_maker",
            f.when(
                f.upper(f.col("grouped_role__c")) == "DECISION MAKER",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .select(
            df_contact_raw["id"].alias("contact_id"),
            df_contact_raw["accountid"].alias("contact_account_id"),
            df_contact_raw["firstname"].alias("contact_first_name"),
            df_contact_raw["lastname"].alias("contact_last_name"),
            df_contact_raw["mailingcity"].alias("contact_city"),
            df_contact_raw["mailingstate"].alias("contact_state"),
            df_contact_raw["mailingcountry"].alias("contact_country"),
            df_contact_raw["cargo__c"].alias("contact_job_title"),
            df_contact_raw["area_setor__c"].alias("contact_company_industry"),
            df_contact_raw["status_do_contato__c"].alias("contact_status"),
            df_contact_raw["grouped_area__c"].alias("contact_grouped_area"),
            f.col("contact_is_decision_maker"),
            df_contact_raw["leadsource"].alias("contact_source"),
            df_contact_raw["isdeleted"].alias("contact_is_deleted"),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_dim_contact_action = (
        df_dim_contact_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_contact_old.filter(f.col("end_date").isNull()).select(
                "contact_id", "hash_dim"
            ),
            "contact_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_contact_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_contact_old["hash_dim"].isNotNull())
                & (df_dim_contact_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_contact_new = (
        df_dim_contact_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_contact_old.join(
                df_dim_contact_action.select("contact_id", "action").filter(
                    f.col("action") == "U"
                ),
                "contact_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(f.col("action").isNull(), df_dim_contact_old["end_date"])
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_contact_old["end_date"],
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

    df_dim_contact_new = jobExec.select_dataframe_columns(
        spark, df_dim_contact_new, table_columns
    )
    df_dim_contact_new = df_dim_contact_new.repartition(num_partitions, "contact_id")

    df_dim_contact_new.write.insertInto(
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
