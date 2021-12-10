import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, auditory_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    job_args.target_schema if job_args.target_schema else jobControl.database_auditory
)


def main():
    df_log_raw = spark.table(f"{jobExec.source_schema}.{jobExec.source_table}").filter(
        f.col("dt") == jobExec.reference_date
    )

    df_log_raw = df_log_raw.distinct()

    df_log_raw = auditory_utils.clean_presto_event_optional_string(df_log_raw)

    df_log_raw = (
        df_log_raw.withColumnRenamed("dt", "reference_date")
        .withColumnRenamed("user", "presto_user")
        .withColumnRenamed("source", "query_source")
    )

    df_log_raw = auditory_utils.extract_metabase_id_from_presto_event(
        df_log_raw, "query_source", "query_text"
    )

    df_metabase = spark.table("metabase.core_user").select(
        f.col("id").alias("metabase_user_id"),
        f.col("first_name").alias("user_first_name"),
        f.col("last_name").alias("user_last_name"),
        f.col("email").alias("user_email"),
    )

    df_log_enriched_stg = df_log_raw.join(df_metabase, ["metabase_user_id"], how="left")

    df_log_enriched_stg = auditory_utils.enrich_user_name_from_presto_user(
        df_log_enriched_stg,
        "presto_user",
        "query_source",
        "user_first_name",
        "user_last_name",
    )

    df_log_enriched_stg = auditory_utils.trim_string_white_spaces(
        df_log_enriched_stg, "user_first_name", "user_last_name"
    )

    df_log_enriched_stg = auditory_utils.build_user_full_name(
        df_log_enriched_stg,
        "user_full_name",
        "user_first_name",
        "user_last_name",
    )

    df_log_enriched_stg = auditory_utils.replace_empty_string_with_null(
        df_log_enriched_stg,
        "user_first_name",
        "user_last_name",
        "user_full_name",
    )

    df_log_enriched = auditory_utils.build_user_email_from_name(
        df_log_enriched_stg,
        "query_source",
        "user_email",
        "user_first_name",
        "user_last_name",
    )

    df_log_enriched = auditory_utils.select_target_columns(
        spark, df_log_enriched, jobExec.target_schema, jobExec.target_table
    )

    hdfs_unique_path = auditory_utils.generate_unique_hdfs_path(jobExec.reference_date)

    s3_table_location = auditory_utils.get_hive_table_location(
        spark, jobExec.target_schema, jobExec.target_table
    )

    auditory_utils.hdfs_to_s3(
        spark,
        df_log_enriched,
        hdfs_unique_path,
        s3_table_location,
        jobExec.target_schema,
        jobExec.target_table,
        jobExec.reference_date,
    )

    auditory_utils.repair_hive_table(spark, jobExec.target_schema, jobExec.target_table)

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .count()
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=False,
        infer_partitions=False,
    )
