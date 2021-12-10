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
        spark, jobExec.database_dm_salesforce, jobExec.target_table
    )

    hash_columns = [
        "opportunity_stage_name",
    ]

    select_columns = [
        "opportunity_funnel_id",
        "opportunity_funnel_date",
        "opportunity_id",
        "opportunity_account_id",
        "opportunity_owner_user_id",
        "opportunity_bdr_user_id",
        "opportunity_stage_name",
        "reference_date",
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

    df_dim_opportunity_old = (
        spark.table(f"{jobExec.database_dm_salesforce}.dim_opportunity")
        .filter(
            (f.col("reference_date") == jobExec.last_reference_date)
            & (f.col("end_date").isNull())
        )
        .withColumn("hash_dim", f.hash(*hash_columns))
        .select("opportunity_id", "hash_dim")
    )

    df_fact_opportunity_funnel_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)
    df_dim_opportunity_old.cache()

    df_stg_fact_opportunity_funnel_delta = (
        df_opportunity_raw.withColumn(
            "opportunity_funnel_id",
            f.hash(f.col("id"), f.lit(jobExec.reference_date), f.col("stagename")),
        )
        .withColumn(
            "opportunity_funnel_date",
            f.date_format(f.col("lastmodifieddate"), "yyyyMMdd").cast(IntegerType()),
        )
        .select(
            f.col("opportunity_funnel_id"),
            f.col("opportunity_funnel_date"),
            df_opportunity_raw["id"].alias("opportunity_id"),
            df_opportunity_raw["accountid"].alias("opportunity_account_id"),
            df_opportunity_raw["ownerid"].alias("opportunity_owner_user_id"),
            df_opportunity_raw["bdr__c"].alias("opportunity_bdr_user_id"),
            df_opportunity_raw["stagename"].alias("opportunity_stage_name"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_stg_fact_opportunity_funnel_action = (
        df_stg_fact_opportunity_funnel_delta.withColumn(
            "hash_stg", f.hash(*hash_columns)
        )
        .join(df_dim_opportunity_old, "opportunity_id", "left")
        .withColumn(
            "action",
            f.when(df_dim_opportunity_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_opportunity_old["hash_dim"].isNotNull())
                & (df_dim_opportunity_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .select(*select_columns)
    )

    df_fact_opportunity_funnel_new = df_stg_fact_opportunity_funnel_action.union(
        df_fact_opportunity_funnel_old.withColumn(
            "reference_date",
            f.lit(jobExec.reference_date).cast(IntegerType()),
        )
    )

    df_fact_opportunity_funnel_new = jobExec.select_dataframe_columns(
        spark, df_fact_opportunity_funnel_new, table_columns
    )
    df_fact_opportunity_funnel_new = df_fact_opportunity_funnel_new.repartition(
        num_partitions, "opportunity_funnel_id"
    )

    df_fact_opportunity_funnel_new.write.insertInto(
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
