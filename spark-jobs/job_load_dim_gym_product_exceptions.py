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
    jobExec.target_schema if jobExec.target_schema else jobExec.database_edw
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    hash_columns = ["expiration_at", "enabled"]

    df_gym_product_exceptions = (
        spark.table(f"{jobExec.database_replica_full_history}.gym_product_exceptions")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .select(
            "id",
            "person_id",
            "company_id",
            "gym_product_id",
            "min_monthly_value",
            "gym_id",
            "exception_gym_product_id",
            "max_visit_value",
            "max_monthly_value",
            "enabled_at",
            "expiration_at",
            "enabled",
            "display_order",
            "site_id",
            "country_id",
            "locale_id",
            "currency_id",
            "created_at",
            "updated_at",
        )
    )

    df_dim_gym_product_exceptions_old = spark.table(
        f"{jobExec.target_schema}.{jobExec.target_table}"
    ).filter(f.col("reference_date") == jobExec.last_reference_date)

    df_dim_gym_product_exceptions_old = df_dim_gym_product_exceptions_old.withColumn(
        "hash_dim", f.hash(*hash_columns)
    )
    df_dim_gym_product_exceptions_old.cache()

    df_dim_gym_product_exceptions_full = (
        df_gym_product_exceptions.withColumn(
            "start_date", f.lit(jobExec.reference_date).cast(IntegerType())
        )
        .withColumn("end_date", f.lit(None).cast(IntegerType()))
        .withColumn(
            "reference_date",
            f.lit(jobExec.reference_date).cast(IntegerType()),
        )
        .withColumn("hash_stg", f.hash(*hash_columns))
    )

    df_stg_gym_product_exceptions_action = (
        df_dim_gym_product_exceptions_full.withColumn("hash_stg", f.hash(*hash_columns))
        .join(
            df_dim_gym_product_exceptions_old.filter(f.col("end_date").isNull()).select(
                "id", "hash_dim"
            ),
            "id",
            "left",
        )
        .withColumn(
            "action",
            f.when(
                df_dim_gym_product_exceptions_old["hash_dim"].isNull(),
                f.lit("I"),
            ).when(
                (df_dim_gym_product_exceptions_old["hash_dim"].isNotNull())
                & (df_dim_gym_product_exceptions_old["hash_dim"] != f.col("hash_stg")),
                f.lit("U"),
            ),
        )
        .filter(f.col("action").isin("I", "U"))
        .drop("hash_dim", "hash_stg")
    )

    df_dim_gym_product_exceptions_new = (
        df_stg_gym_product_exceptions_action.filter(f.col("action").isin("I", "U"))
        .drop("action")
        .union(
            df_dim_gym_product_exceptions_old.join(
                df_stg_gym_product_exceptions_action.select("id", "action").filter(
                    f.col("action") == "U"
                ),
                "id",
                "left",
            )
            .withColumn(
                "end_date",
                f.when(
                    f.col("action").isNull(),
                    df_dim_gym_product_exceptions_old["end_date"],
                )
                .when(
                    (f.col("action").isNotNull() & f.col("end_date").isNotNull()),
                    df_dim_gym_product_exceptions_old["end_date"],
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

    df_dim_gym_product_exceptions_new = jobExec.select_dataframe_columns(
        spark, df_dim_gym_product_exceptions_new, table_columns
    )
    df_dim_gym_product_exceptions_new = df_dim_gym_product_exceptions_new.repartition(
        num_partitions, "id"
    )

    df_dim_gym_product_exceptions_new.write.insertInto(
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
