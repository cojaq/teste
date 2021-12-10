import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 6

jobExec = jobControl.Job(job_name, job_args)


def main():
    df = spark.table(f"{jobExec.source_schema}.{jobExec.source_table}")
    if jobExec.get_incremental:
        jobExec.totalLines = df.filter(
            f.col("reference_date") == jobExec.reference_date
        ).count()
    else:
        jobExec.totalLines = df.count()

    if jobExec.totalLines > 0:
        table_location = dataframe_utils.returnHiveTableLocation(
            spark,
            jobExec.source_schema,
            jobExec.source_table,
            jobExec.get_incremental,
            jobExec.reference_date,
        )
        if not jobExec.load_incremental:
            jobExec.deleteTable(
                schema=jobExec.target_schema, table=jobExec.target_table
            )
        jobExec.redshift.LoadS3toRedshift(
            table_location, jobExec.target_schema, jobExec.target_table
        )

    else:
        jobExec.logger.warning("Target table is empty")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main)
