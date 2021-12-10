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

    hash_columns = ["user_profile_name", "user_role_name", "user_is_active"]

    df_user_raw = (
        spark.table(f"{jobExec.database_salesforce_raw}.user")
        .filter(
            (f.col("reference_date") == jobExec.reference_date)
            & (f.lower(f.col("id")) != "id")
        )
        .replace("null", None)
        .replace("", None)
    )

    df_profile = spark.table(f"{jobExec.database_salesforce}.profile")

    df_user_role = spark.table(f"{jobExec.database_salesforce}.user_role")

    df_dim_user_old = spark.table(
        f"{jobExec.database_dm_salesforce}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_user_old = df_dim_user_old.withColumn(
        "hash_dim", f.hash(f.concat(*hash_columns))
    )
    df_dim_user_old.cache()

    df_dim_user_full = (
        df_user_raw.join(
            df_profile, df_user_raw["profileid"] == df_profile["id"], "left"
        )
        .join(
            df_user_role,
            df_user_raw["userroleid"] == df_user_role["id"],
            "left",
        )
        .withColumn("start_date", f.lit(jobExec.reference_date).cast(IntegerType()))
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .select(
            df_user_raw["id"].alias("user_id"),
            df_user_raw["firstname"].alias("user_first_name"),
            df_user_raw["lastname"].alias("user_last_name"),
            df_user_raw["companyname"].alias("user_company_name"),
            df_user_raw["division"].alias("user_division_name"),
            df_user_raw["department"].alias("user_department_name"),
            df_user_raw["title"].alias("user_title_name"),
            df_user_raw["city"].alias("user_city_name"),
            df_user_raw["state"].alias("user_state_name"),
            df_user_raw["country"].alias("user_country_name"),
            df_user_raw["statecode"].alias("user_state_code"),
            df_user_raw["countrycode"].alias("user_country_code"),
            df_user_raw["isactive"].alias("user_is_active"),
            df_profile["name"].alias("user_profile_name"),
            df_user_role["name"].alias("user_role_name"),
            df_user_raw["defaultcurrencyisocode"].alias("user_currency_iso_code"),
            df_user_raw["usertype"].alias("user_type"),
            df_user_raw["managerid"].alias("user_manager_user_id"),
            f.col("start_date"),
            f.col("end_date"),
            f.lit(jobExec.reference_date).cast(IntegerType()).alias("reference_date"),
        )
    )

    df_dim_user_action = (
        df_dim_user_full.withColumn("hash_stg", f.hash(f.concat(*hash_columns)))
        .join(
            df_dim_user_old.filter(f.col("end_date").isNull()).select(
                "user_id", "hash_dim"
            ),
            "user_id",
            "left",
        )
        .withColumn(
            "action",
            f.when(df_dim_user_old["hash_dim"].isNull(), f.lit("I")).when(
                (df_dim_user_old["hash_dim"].isNotNull())
                & (df_dim_user_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_user_new = (
        df_dim_user_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_user_old.join(
                df_dim_user_action.select("user_id", "action").filter(
                    f.col("action") == "U"
                ),
                "user_id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(f.col("action").isNull(), df_dim_user_old["end_date"])
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_user_old["end_date"],
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

    df_dim_user_new = jobExec.select_dataframe_columns(
        spark, df_dim_user_new, table_columns
    )
    df_dim_user_new = df_dim_user_new.repartition(num_partitions, "user_id")

    df_dim_user_new.write.insertInto(
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
