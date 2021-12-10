import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 6

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema if jobExec.target_schema else jobExec.database_work
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    # Reading source table from ODS MariaDB
    df_people = (
        spark.table(f"{jobExec.database_replica_full}.people")
        .select(
            "id",
            "email",
            "gym_financial_notifications",
            "company_financial_notifications",
        )
        .filter(
            (f.col("company_financial_notifications") == 1)
            | (f.col("gym_financial_notifications") == 1)
        )
    )

    df_countries = spark.table(f"{jobExec.database_replica_full}.countries").select(
        "gp_company_id"
    )

    df_company_members = (
        spark.table(f"{jobExec.database_replica_full}.company_members")
        .select("person_id", "company_id", "enabled")
        .filter(f.col("enabled") == 1)
    )

    df_emails = (
        spark.table(f"{jobExec.database_replica_full}.emails")
        .select("email_address", "emailable_type", "enabled")
        .filter(
            (f.col("enabled") == 1) & (f.col("emailable_type").isin("Gym", "Company"))
        )
    )

    df_gympass_user = df_company_members.join(
        df_countries.withColumnRenamed("gp_company_id", "company_id"),
        "company_id",
        "inner",
    ).select("person_id", f.lit("GYMPASS USER").alias("person_type"))

    df_gym_company_user = (
        df_people.withColumnRenamed("id", "person_id")
        .join(
            df_emails.withColumnRenamed("email_address", "email"),
            "email",
            "inner",
        )
        .withColumn(
            "person_type",
            f.when(f.col("emailable_type") == "Gym", f.lit("GYM USER")).when(
                f.col("emailable_type") == "Company", f.lit("COMPANY USER")
            ),
        )
        .select("person_id", "person_type")
    )

    df_stg_person_type = df_gympass_user.union(df_gym_company_user)

    df_stg_person_type = jobExec.select_dataframe_columns(
        spark, df_stg_person_type, table_columns
    )
    df_stg_person_type = df_stg_person_type.repartition(num_partitions, "person_id")

    df_stg_person_type.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
