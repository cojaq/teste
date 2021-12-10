import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
jobExec = jobControl.Job(job_name, job_args)

table_location = jobExec.table_location


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )
    df_identity = spark.table(f"{jobExec.source_schema}.{jobExec.target_table}").filter(
        (f.col("dt") == jobExec.reference_date)
    )

    df_identity = jobExec.select_dataframe_columns(spark, df_identity, table_columns)

    count_lines = df_identity.count()

    jobExec.totalLines = count_lines
    num_partitions = jobExec.calcNumPartitions()

    if jobExec.totalLines == 0:
        jobExec.logger.warning("Skipping extract job because query return zero results")
    else:

        df_identity = df_identity.repartition(
            num_partitions, jobExec.repartition_column
        )

        s3_save_path = dataframe_utils.returnHiveTableLocation(
            spark, jobExec.target_schema, jobExec.target_table
        )

        df_identity.write.partitionBy("event_type", "dt").parquet(
            s3_save_path, mode="overwrite"
        )

        for row in df_identity.select("event_type").distinct().collect():
            partition_args = "event_type='{}',dt='{}'".format(
                row.asDict()["event_type"], jobExec.reference_date
            )
            relative_path = "event_type={}/dt={}".format(
                row.asDict()["event_type"], jobExec.reference_date
            )
            partition_statement = "ALTER TABLE {}.`{}` ADD IF NOT EXISTS PARTITION ({}) LOCATION '{}/{}/'".format(
                jobExec.target_schema,
                jobExec.target_table,
                partition_args,
                s3_save_path,
                relative_path,
            )
            spark.sql(partition_statement)


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True)
