import json
import os
from datetime import datetime

import zendesk_model
from jobControl import jobControl

# Third Part libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, lit
from utils import arg_utils

###############################################################################


def _info(msg):
    jobExec.logger.info(msg)
    # print(f'{datetime.now()} {msg}')


def _error(msg):
    jobExec.logger.error(msg)
    # print(f'{datetime.now()} ERROR')
    # print(f'{datetime.now()} {msg}')


###############################################################################


def load(base, kwargs):

    schema = zendesk_model.model[base]["schema"]
    pk_list = zendesk_model.model[base]["pk_cols"]
    part_list = zendesk_model.model[base]["part_cols"]

    spark = kwargs["spark"]
    transform_bucket = kwargs["transform_bucket"]
    transform_prefix = kwargs["transform_prefix"]
    transform_path = f"hdfs:///tmp/{transform_bucket}/{transform_prefix}{base}/"
    load_bucket = kwargs["load_bucket"]
    load_prefix = kwargs["load_prefix"]
    load_path = f"s3://{load_bucket}/{load_prefix}{base}/"

    _info(f"Getting partitions from {transform_path}...")
    try:
        stg_parts = (
            spark.read.schema(zendesk_model.get_transform_schema(base))
            .parquet(transform_path)
            .select(part_list)
            .distinct()
            .sort(array(part_list), ascending=False)
            .collect()
        )
    except Exception as e:
        if "Unable to infer schema" in str(e):
            _info(f"WARNING: {transform_path} is empty.")
            return
        else:
            _error(str(e))
            raise e

    for stg_part in stg_parts:

        part_path = (
            "/".join([f"{part_col}={stg_part[part_col]}" for part_col in part_list])
            + "/"
        )

        _info(f"Reading {part_path} from {transform_path}...")
        stg_df = (
            spark.read.schema(zendesk_model.get_transform_schema(base))
            .option("basePath", transform_path)
            .parquet(f"{transform_path}{part_path}")
            .drop("_transform_datetime")
        )

        _info(f"  Overwriting {load_path}{part_path}...")

        # When persisting ".parquet" data it is important to order the columns
        # in the same way as the DDL(hive) command
        stg_df.withColumn(
            "_load_datetime", lit(str(datetime.utcnow())).cast("timestamp")
        ).write.mode("overwrite").parquet(f"{load_path}{part_path}")

        _info(f"  Partition {load_path}{part_path} overwrite successfully.")


###############################################################################


def main():

    _info("Start")

    app_args = json.loads(job_args.zendesk_args)

    kwargs = {
        "spark": spark,
        "transform_bucket": app_args["transform_bucket"],
        "transform_prefix": app_args["transform_prefix"]
        if app_args["transform_prefix"][-1:] == "/"
        else app_args["transform_prefix"] + "/",
        "load_bucket": app_args["load_bucket"],
        "load_prefix": app_args["load_prefix"]
        if app_args["load_prefix"][-1:] == "/"
        else app_args["load_prefix"] + "/",
    }

    load(app_args["base"], kwargs)

    _info("End")


###############################################################################

if __name__ == "__main__":

    job_args = arg_utils.get_job_args()
    job_name = os.path.basename(__file__).split(".")[0]

    jobExec = jobControl.Job(job_name, job_args)

    spark = SparkSession.builder.appName(job_name).getOrCreate()

    jobExec.execJob(
        main,
        spark,
        add_hive_path=False,
        delete_excessive_files=False,
        infer_partitions=False,
    )
