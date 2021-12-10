import os
from urllib.parse import parse_qs

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 6

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema if jobExec.target_schema else jobExec.database_work
)


def get_schema():
    fields = [
        StructField("key_query_string", StringType(), True),
        StructField("value_query_string", StringType(), True),
    ]
    return StructType(fields=fields)


def splitQueryString(string):
    dict_url = parse_qs(string[string.find("?") + 1 :])

    list_query_string = []
    for key, value in dict_url.items():
        if not value:
            value = "EMPTY"
        list_query_string.append([key, value[0]])

    return list_query_string


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    udf_split = f.udf(lambda x: splitQueryString(x), ArrayType(get_schema()))

    # Reading source table from ODS MariaDB
    df_page_views = (
        spark.table(f"{jobExec.database_replica_full_history}.page_views")
        .filter(f.col("reference_date") == jobExec.reference_date)
        .select("id", "request_url")
    )

    df_stg_page_views_query_string = (
        df_page_views.filter(f.col("request_url").like("%?%"))
        .withColumn("splited_url", f.explode(udf_split("request_url")))
        .select(
            f.col("id").alias("page_view_id"),
            f.col("splited_url").getItem("key_query_string").alias("key_query_string"),
            f.col("splited_url")
            .getItem("value_query_string")
            .alias("value_query_string"),
        )
    )

    df_stg_page_views_query_string = df_stg_page_views_query_string.repartition(
        num_partitions, "page_view_id"
    )

    df_stg_page_views_query_string = dataframe_utils.createPartitionColumns(
        df_stg_page_views_query_string, jobExec.reference_date
    )

    df_stg_page_views_query_string = jobExec.select_dataframe_columns(
        spark, df_stg_page_views_query_string, table_columns
    )
    df_stg_page_views_query_string.repartition(num_partitions, "page_view_id")

    df_stg_page_views_query_string.write.insertInto(
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
