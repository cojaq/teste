import json
import os
from datetime import datetime

import boto3
import zendesk_model
from jobControl import jobControl

# Third Part libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, first, last, lit, max, min, udf
from pyspark.sql.types import BooleanType, FloatType, StringType
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


def s3_path_exists(bucket, path):
    for object_summary in bucket.objects.filter(Prefix=path):
        if object_summary.key.endswith(".parquet"):
            return True

    return False


###############################################################################


def range_SLA(duration_first_reply, timestamp):

    chat_created_at = timestamp.strftime("%A")

    def chat_first_reply_time_days(duration_first_reply):
        return duration_first_reply / 60 / 60 / 24

    def SLA_email(duration_first_reply, chat_created_at):
        if duration_first_reply is not None and chat_created_at is not None:
            if (
                chat_created_at == "Friday"
                and chat_first_reply_time_days(duration_first_reply) <= 4
            ):
                return 1
            elif (
                chat_created_at == "Saturday"
                and chat_first_reply_time_days(duration_first_reply) <= 3
            ):
                return 1
            elif (
                chat_created_at == "Sunday"
                and chat_first_reply_time_days(duration_first_reply) <= 2
            ):
                return 1
            else:
                return chat_first_reply_time_days(duration_first_reply)
        else:
            return None

    sla_result = SLA_email(duration_first_reply, chat_created_at)

    if sla_result == None:
        return "no reply"
    elif sla_result <= 1:
        return "24hrs"
    else:
        return "> 24hrs"


def duration_wait_time(args):
    (
        chat_type,
        sender_type,
        missed,
        start_time,
        end_time,
        duration_first_reply,
    ) = args

    diff = None
    if chat_type == "chat.msg" and sender_type == "Visitor" and missed:
        diff = (end_time - end_time).total_seconds()
    else:
        diff = duration_first_reply
    return diff


good_satisfaction_chat_udf = udf(
    lambda rating: True if rating == "good" else False, BooleanType()
)

bad_satisfaction_chat_udf = udf(
    lambda rating: True if rating == "bad" else False, BooleanType()
)

transferred_chat_udf = udf(
    lambda chat_type: True if chat_type == "chat.transfer.department" else False,
    BooleanType(),
)

completed_chat_udf = udf(
    lambda sender_type: True if sender_type == "Agent" else False,
    BooleanType(),
)

served_chat_udf = udf(lambda missed: True if missed == True else False, BooleanType())

duration_ufd = udf(
    lambda start_time, end_time: (end_time - start_time).total_seconds(),
    FloatType(),
)

duration_wait_time_udf = udf(lambda *args: duration_wait_time(args), FloatType())

range_sla_udf = udf(
    lambda duration_first_reply, timestamp: range_SLA(duration_first_reply, timestamp),
    StringType(),
)


def _chat(df, kwargs):
    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]

    _info("Reading chat history df")
    # join history with chats
    history_df = (
        spark.read.schema(zendesk_model.get_extract_schema("chat_history"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}chat_history/")
        .select("chat_id", "type", "sender_type", "timestamp")
    )

    _info("Calculating custom fields")
    chat_df = (
        df.select("chat_id", "duration_first_reply", "rating", "missed")
        .join(history_df, ["chat_id"], "left")
        .groupBy("chat_id")
        .agg(
            good_satisfaction_chat_udf(first(col("rating"))).alias(
                "good_satisfaction_chat"
            ),
            bad_satisfaction_chat_udf(first(col("rating"))).alias(
                "bad_satisfaction_chat"
            ),
            transferred_chat_udf(first(col("type"))).alias("transferred_chat"),
            completed_chat_udf(last(col("sender_type"))).alias("completed_chat"),
            served_chat_udf(first(col("missed"))).alias("served_chat"),
            duration_ufd(min(col("timestamp")), max(col("timestamp"))).alias(
                "duration"
            ),
            duration_wait_time_udf(
                last(col("type")),
                last(col("sender_type")),
                first(col("missed")),
                min(col("timestamp")),
                max(col("timestamp")),
                first(col("duration_first_reply")),
            ).alias("duration_wait_time"),
            range_sla_udf(
                first(col("duration_first_reply")), first(col("timestamp"))
            ).alias("duration_range_sla"),
        )
    )

    _info("Start: Join df with chat_df")

    df = df.drop(
        "good_satisfaction_chat",
        "bad_satisfaction_chat",
        "transferred_chat",
        "transferred_chat",
        "completed_chat",
        "served_chat",
        "duration",
        "duration_wait_time",
        "duration_range_sla",
    ).join(chat_df, ["chat_id"], "left")

    _info("End: Join df with chat_df")

    return df


def transform(base, kwargs):

    schema = zendesk_model.model[base]["schema"]
    pk_list = zendesk_model.model[base]["pk_cols"]
    part_list = zendesk_model.model[base]["part_cols"]

    specify = None
    if base == "chat":
        specify = _chat

    spark = kwargs["spark"]
    extract_strategy = kwargs["extract_strategy"]
    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]
    extract_path = f"s3://{extract_bucket}/{extract_prefix}{base}/"
    transform_bucket = kwargs["transform_bucket"]
    transform_prefix = kwargs["transform_prefix"]
    transform_path = f"hdfs:///tmp/{transform_bucket}/{transform_prefix}{base}/"
    load_bucket = kwargs["load_bucket"]
    load_prefix = kwargs["load_prefix"]
    load_path = f"s3://{load_bucket}/{load_prefix}{base}/"

    # Extract

    _info(f"Reading {extract_path}...")
    try:
        raw_df = spark.read.schema(zendesk_model.get_extract_schema(base)).parquet(
            extract_path
        )
    except Exception as e:
        if "Unable to infer schema" in str(e):
            _info(f"WARNING: {extract_path} is empty.")
            return
        else:
            _error(str(e))
            raise e

    _info(f"  Dropping Duplicates...")
    raw_df = (
        raw_df.sort("_extract_timestamp", ascending=False)
        .dropDuplicates(pk_list)
        .drop("_extract_page", "_extract_timestamp")
    )

    if specify:
        raw_df = specify(raw_df, kwargs)

    if extract_strategy == "finale":
        # Se for finale le do raw e salva direto no refined

        _info(f"Overwriting {load_path}...")

        raw_df.withColumn(
            "_load_datetime", lit(str(datetime.utcnow())).cast("timestamp")
        ).write.mode("overwrite").partitionBy(part_list).parquet(load_path)

        _info(f"Parquet {load_path} overwrite successfully.")

    else:

        # Transform

        _info(f"Writing {transform_path}...")

        raw_df.withColumn(
            "_transform_datetime",
            lit(str(datetime.utcnow())).cast("timestamp"),
        ).write.mode("overwrite").partitionBy(part_list).parquet(transform_path)

        _info(f"Parquet {transform_path} overwrite successfully.")

        # Load

        # Create s3 client instance
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(load_bucket)

        # Create list to store partition path
        old_partitions = []

        # format s3 path
        raw_parts = (
            raw_df.select(part_list)
            .distinct()
            .sort(array(part_list), ascending=False)
            .collect()
        )
        for raw_part in raw_parts:

            part_path = (
                "/".join([f"{part_col}={raw_part[part_col]}" for part_col in part_list])
                + "/"
            )

            if "=None/" in part_path:
                continue

            _info(f"Raw data partition:\t\t{part_path} ")
            if s3_path_exists(bucket, f"{load_prefix}{base}/{part_path}") is True:
                old_partitions.append(f"{load_path}{part_path}")
                _info(f"Partition exists:\t\t{part_path}")
            else:
                _info(f"Partition does not exist, will be added:\t\t{part_path}")

        if len(old_partitions) > 0:
            # Read the partitions that exist
            try:
                old_df = spark.read.schema(zendesk_model.get_load_schema(base)).parquet(
                    *old_partitions
                )
            except Exception as e:
                if "Path does not exist" in str(e) or "Unable to infer schema" in str(
                    e
                ):
                    _info(f"  WARNING:  does not exist or is empty.")
                else:
                    _error(str(e))
                    raise e

            old_df = (
                old_df.drop("_load_datetime")
                .join(raw_df.select(pk_list), pk_list, "left_anti")
                .withColumn(
                    "_transform_datetime",
                    lit(str(datetime.utcnow())).cast("timestamp"),
                )
            )

            old_count = old_df.count()
            _info(f"  Old records to append: {old_count}.")

            if old_count > 0:

                _info(f"  Appending to {transform_path}...")
                old_df.write.mode("append").partitionBy(part_list).parquet(
                    transform_path
                )

                _info(f"  Partitions appended to {transform_path} successfully.")


###############################################################################


def main():

    _info("Start")

    app_args = json.loads(job_args.zendesk_args)

    kwargs = {
        "spark": spark,
        "extract_strategy": app_args["extract_strategy"],
        "extract_bucket": app_args["extract_bucket"],
        "extract_prefix": app_args["extract_prefix"]
        if app_args["extract_prefix"][-1:] == "/"
        else app_args["extract_prefix"] + "/",
        "transform_bucket": app_args["transform_bucket"],
        "transform_prefix": app_args["transform_prefix"]
        if app_args["transform_prefix"][-1:] == "/"
        else app_args["transform_prefix"] + "/",
        "load_bucket": app_args["load_bucket"],
        "load_prefix": app_args["load_prefix"]
        if app_args["load_prefix"][-1:] == "/"
        else app_args["load_prefix"] + "/",
    }

    transform(app_args["base"], kwargs)

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
