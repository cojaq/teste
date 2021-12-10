import os
import re
from collections import Counter
from datetime import datetime
from functools import reduce

import boto3
from jobControl import jobControl
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, IntegerType
from pyspark.sql.window import Window
from utils import arg_utils, dataframe_utils, date_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 5

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = jobExec.target_schema if jobExec.target_schema else "appsflyer"


jobExec.account_id = "1dc5-acc-sk7ttK2M-1dc5"
jobExec.load_type = "data-locker-hourly"

prefix_pattern = "{account_id}/{load_type}/t={report_name}/dt={fetch_date}"

REFERENCE_COLUMNS = [
    "reference_date",
    "reference_hour",
    "reference_pipeline_status",
]


def check_success_file(full_data):
    """Parse each s3 path to get _SUCCESS file for each path

    Args:
        full_data (s3_full_path): S3 list_objects full path;

    Returns:
        dictionary: dictionary containing hour(int):status(bool) mapping
    """
    load_status = {}
    for elt in full_data["Contents"]:
        split_key = elt["Key"].split("/")
        hour = split_key[-2]
        filename = split_key[-1]

        if filename == "_SUCCESS" or load_status[hour]:
            load_status[hour] = True
            continue
        else:
            load_status[hour] = False

    return load_status


def format_to_date(date_str):
    """Format to Date object

    Args:
        date_str (string): date without dash

    Returns:
        date: date object without dash
    """
    return datetime.strptime(date_str, "%Y%m%d").date()


def insert_reference_date(df, reference_date):
    """Insert Reference Date into DataFrame

    Args:
        df (pyspark DataFrame): Input DataFrame
        reference_date (string): Date - no dash

    Returns:
        pyspark DataFrame: Treated DataFrame with integer column
    """
    return df.withColumn("reference_date", f.lit(int(reference_date)))


def insert_status_columns(df, status_value, hour):
    """Introduces status columns - such as fetched hour and status (SUCCESS File)

    Args:
        df (pyspark DataFrame): Input DataFrame
        status_value (boolean): Status Flag for given hour
        hour (str): Current iterable hour (with h=%H format)

    Returns:
        pyspark DataFrame: Full DataFrame with status columns updated
    """
    df = df.withColumn("reference_hour", f.lit(hour.split("=")[-1]))
    df = df.withColumn("reference_pipeline_status", f.lit(status_value))
    return df


def union_dataframes(*dfs):
    """Union a list of DataFrames input as args

    Returns:
        pyspark DataFrame: Concatenated DataFrame
    """
    return reduce(DataFrame.unionAll, dfs)


def replace_null(column):
    """Replace all "null" strings with None given a column - UDF function

    Args:
        column (string): string to be treated

    Returns:
        string: Treated column as UDF
    """
    return f.when(column != "null", column).otherwise(f.lit(None))


def null_formatting(df):
    """Parse through DF columns and replace null string with proper Null value

    Args:
        df (pyspark DataFrame): Input Dataframe

    Returns:
        pyspark DataFrame: Treated DataFrame
    """
    for col_name in df.columns:
        if col_name not in REFERENCE_COLUMNS:
            df = df.withColumn(col_name, replace_null(f.col(col_name)))

    return df


def main():
    s3 = boto3.client("s3")

    fetch_date = format_to_date(jobExec.reference_date)
    jobExec.logger.info(f"Job Exec {jobExec.target_table} and Args: {job_args.table}")
    prefix = prefix_pattern.format(
        account_id=jobExec.account_id,
        load_type=jobExec.load_type,
        report_name=jobExec.target_table,
        fetch_date=fetch_date,
    )

    jobExec.logger.info(
        "Bucket Name:{} - with Prefix: {} ".format(job_args.raw_bucket, prefix)
    )

    s3_data = s3.list_objects_v2(Bucket=job_args.raw_bucket, Prefix=prefix)
    if "Contents" in s3_data:
        status = check_success_file(s3_data)

        jobExec.logger.info(f"Fetching data for date {fetch_date}")
        jobExec.logger.info(f"Current Status is: {status}")

        df_list = []
        sum_of_hours = 0
        # Reading File
        for hour, status_value in status.items():
            jobExec.logger.info(f"Fetching the HOUR: {hour}")
            file_path = f"s3a://{job_args.raw_bucket}/{prefix}/{hour}/*.gz"
            df = spark.read.option("header", "true").csv(file_path)

            df = insert_status_columns(df, status_value, hour)
            sum_of_hours += df.count()

            # Create a List of Dataframes
            df_list.append(df)

        # Concat DataFrames
        df_concat = union_dataframes(*df_list)

        # Insert Reference Date
        df_concat = insert_reference_date(df_concat, jobExec.reference_date)
        jobExec.logger.info("Total row count is: {}".format(df_concat.count()))

        # Scale Columns before inserting
        jobExec.logger.info("The date is: {}".format(jobExec.reference_date))
        table_columns = dataframe_utils.return_hive_table_columns(
            spark, jobExec.target_schema, jobExec.target_table
        )
        df_column_scaled = jobExec.select_dataframe_columns(
            spark, df_concat, table_columns
        )

        jobExec.logger.info("Writing Schema:")
        jobExec.logger.info(df_column_scaled.printSchema())

        df_column_scaled = null_formatting(df_column_scaled)

        df_column_scaled.repartition(num_partitions).write.insertInto(
            f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
        )
        jobExec.totalLines = (
            spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
        ).count()

        jobExec.logger.info(df_concat.printSchema())
        jobExec.logger.info(df_column_scaled.printSchema())
    elif s3_data["ResponseMetadata"].get("HTTPStatusCode", 404) == 404:
        jobExec.logger.warning(
            f"Amazon Bucket Fetch haven't worked: {jobExec.target_table} -  Date: {fetch_date}"
        )
        jobExec.logger.info(s3_data)
        raise Exception(s3_data)
    else:
        jobExec.logger.warning(
            f"Target property has no data for this day ({fetch_date})"
        )
        jobExec.totalLines = 0
        jobExec.logger.info("No rows are inserted - table not found")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    jobExec.execJob(main, spark, add_hive_path=True, infer_partitions=True)
