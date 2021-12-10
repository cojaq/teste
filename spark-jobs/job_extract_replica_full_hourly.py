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
    jobExec.target_schema
    if jobExec.target_schema
    else jobExec.database_replica_full_hourly
)


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    id_column = (
        f"{jobExec.target_table[:-1]}_id"
        if jobExec.target_table[-1] == "s"
        else f"{jobExec.target_table}_id"
    )
    query_columns = table_columns.copy()
    if id_column in query_columns:
        query_columns.remove(id_column)
    query = f"(SELECT {','.join(query_columns)} FROM {jobExec.target_table}) AS query"

    # Reading source table from Replica Full

    dict_source_table = dict(options=dict(dbtable=query))

    df_extraction = jobExec.sparkReadJdbc(spark, dict_source_table)

    # Write data to target table
    df_extraction = df_extraction.withColumn(id_column, f.col("id"))
    df_extraction = jobExec.select_dataframe_columns(
        spark, df_extraction, table_columns
    )
    df_extraction.createOrReplaceTempView("df_extraction")
    spark.sql(
        f"INSERT OVERWRITE TABLE {jobExec.target_schema}.{jobExec.target_table} SELECT * FROM df_extraction"
    )

    jobExec.totalLines = spark.table(
        f"{jobExec.target_schema}.{jobExec.target_table}"
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
