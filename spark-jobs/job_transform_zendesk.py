import json
import os
from datetime import datetime

import boto3
import zendesk_model
from jobControl import jobControl

# Third Part libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    explode_outer,
    from_json,
    lit,
    regexp_replace,
    schema_of_json,
)
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


def delete_hdfs_path(spark, path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)


def s3_path_exists(bucket, path):
    for object_summary in bucket.objects.filter(Prefix=path):
        if object_summary.key.endswith(".parquet"):
            return True

    return False


###############################################################################


def _org_field(df, kwargs):

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]

    field_df = (
        spark.read.schema(zendesk_model.get_extract_schema("cf_org"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}cf_org/")
        .drop("_extract_page", "_extract_timestamp")
    )

    return df.drop("title", "type", "description", "raw_title", "raw_description").join(
        field_df, ["field_id"], "left"
    )


###############################################################################


def _user_field(df, kwargs):

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]

    field_df = (
        spark.read.schema(zendesk_model.get_extract_schema("cf_user"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}cf_user/")
        .drop("_extract_page", "_extract_timestamp")
    )

    return df.drop("title", "type", "description", "raw_title", "raw_description").join(
        field_df, ["field_id"], "left"
    )


###############################################################################


def _ticket_field(df, kwargs):

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]

    field_df = (
        spark.read.schema(zendesk_model.get_extract_schema("cf_ticket"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}cf_ticket/")
        .drop("_extract_page", "_extract_timestamp")
    )

    return df.drop(
        "title",
        "type",
        "description",
        "raw_title",
        "raw_description",
        "custom_field_options",
    ).join(field_df, ["field_id"], "left")


###############################################################################


def _ticket(df, kwargs):

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]
    depara_path = kwargs["depara_path"]
    depara_fields_path = kwargs["depara_fields_path"]

    form_df = (
        spark.read.schema(zendesk_model.get_extract_schema("form"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}form/")
        .drop("_extract_page", "_extract_timestamp")
    )

    df = df.drop("form_name", "form_tags").join(form_df, ["form_id"], "left")

    field_df = (
        spark.read.schema(zendesk_model.get_extract_schema("ticket_field"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}ticket_field/")
        .drop(
            "title",
            "type",
            "description",
            "raw_title",
            "raw_description",
            "custom_field_options",
            "_extract_page",
            "_extract_timestamp",
        )
    )

    cf_df = (
        spark.read.schema(zendesk_model.get_extract_schema("cf_ticket"))
        .parquet(f"s3://{extract_bucket}/{extract_prefix}cf_ticket/")
        .drop("_extract_page", "_extract_timestamp")
    )

    # Corrects the json data types: None to none, True to true, False to false
    cf_df = cf_df.withColumn(
        "custom_field_options",
        regexp_replace(
            regexp_replace(
                regexp_replace("custom_field_options", "None", "null"),
                "False",
                "false",
            ),
            "True",
            "true",
        ),
    )

    # Get Json schema and apply
    json_schema = schema_of_json(
        cf_df.select("custom_field_options")
        .filter("custom_field_options IS NOT NULL")
        .take(1)[0][0]
    )

    cf_df = cf_df.withColumn(
        "custom_fields", from_json("custom_field_options", json_schema)
    )

    # Create temp views
    df.createOrReplaceTempView("ticket")
    field_df.createOrReplaceTempView("ticket_field")
    cf_df.createOrReplaceTempView("cf_ticket")

    # Read the fields used in the trees
    if depara_fields_path is None:
        msg = "_ticket: Error `depara_fields_path` attribute cannot be empty"
        _error(msg)
        raise Exception(msg)

    depara_fields_df = (
        spark.read.format("csv").option("header", "true").load(depara_fields_path)
    )
    depara_fields_df.createOrReplaceTempView("depara_fields")

    ticket_form_field_df = spark.sql(
        """
        SELECT
            t.ticket_id, t.form_name, fields.value AS field_value, 
                custom_fields.custom_fields
        FROM ticket AS t
        INNER JOIN ticket_field AS fields
            ON t.ticket_id = fields.ticket_id
        INNER JOIN cf_ticket AS custom_fields
            ON fields.field_id = custom_fields.field_id
        INNER JOIN depara_fields AS filter_fields
            ON (custom_fields.field_id = filter_fields.field_id)
        WHERE
            (t.form_name IS NOT NULL) AND
            (filter_fields.form_name = t.form_name)"""
    )

    ticket_form_field_df = ticket_form_field_df.withColumn(
        "custom_fields", explode_outer("custom_fields")
    ).sort("ticket_id", "custom_fields.value")

    # Add and Drop temp views
    spark.catalog.dropTempView("ticket")
    spark.catalog.dropTempView("ticket_field")
    spark.catalog.dropTempView("cf_ticket")

    tree_df = ticket_form_field_df.filter('form_name != "Global Decision Tree"')
    tree_depara_df = None
    if tree_df.count() > 0:
        _info(f"Start transform Ticket form_tag...")
        tree_df.createOrReplaceTempView("tree")

        tree_depara_df = spark.sql(
            f"""
            SELECT 
                ticket_id, FIRST(custom_fields.name) AS `form_tags`
            FROM tree
            WHERE
                field_value = custom_fields.value
            GROUP BY ticket_id"""
        )
        _info(f"End transform Ticket form_tag...")

    global_decision_tree_df = ticket_form_field_df.filter(
        'form_name == "Global Decision Tree"'
    )
    global_decision_tree_depara_df = None
    if global_decision_tree_df.count() > 0:
        _info(f"Start (Historical/Full) transform Ticket form_tag...")
        global_decision_tree_df.createOrReplaceTempView("global_decision_tree")

        if depara_path is None:
            msg = "_ticket: Error `depara_path` attribute cannot be empty"
            _error(msg)
            raise Exception(msg)

        depara_df = spark.read.format("csv").option("header", "true").load(depara_path)

        # Create temp views
        depara_df.createOrReplaceTempView("depara")

        # Group by ticket_id, concat many custom Fields rows into single one
        field_cf_df = spark.sql(
            """
            SELECT 
                ticket_id,
                CONCAT_WS("::", COLLECT_LIST(custom_fields.name)) AS custom_fields_name
            FROM global_decision_tree
            WHERE
                field_value = custom_fields.value
            GROUP BY ticket_id"""
        )

        # Add and Drop temp views
        field_cf_df.createOrReplaceTempView("fields")

        # Prepare tickets
        global_decision_tree_depara_df = spark.sql(
            f"""
            SELECT DISTINCT
                ticket_id,
                para.PARA AS `form_tags`
            FROM fields
            LEFT JOIN depara AS  para
                ON para.DE = fields.custom_fields_name"""
        )

        _info(f"End (Historical/Full) transform Ticket form_tag...")

    full_depara_df = None
    if (tree_depara_df is None) and (global_decision_tree_depara_df is None):
        raise Exception("Error during 'depara' process, no data was processed")
    elif tree_depara_df is None and global_decision_tree_depara_df is not None:
        full_depara_df = global_decision_tree_depara_df
    elif global_decision_tree_depara_df is None and tree_depara_df is not None:
        full_depara_df = tree_depara_df
    else:
        full_depara_df = tree_depara_df.union(global_decision_tree_depara_df)

    df = df.join(full_depara_df, ["ticket_id"], "left")

    # Drop temp views
    spark.catalog.clearCache()

    # Free memory
    del (
        cf_df,
        field_df,
        form_df,
        field_cf_df,
        ticket_form_field_df,
        json_schema,
        tree_df,
        global_decision_tree_df,
        full_depara_df,
    )

    return df


###############################################################################


def transform(base, kwargs):

    schema = zendesk_model.model[base]["schema"]
    pk_list = zendesk_model.model[base]["pk_cols"]
    part_list = zendesk_model.model[base]["part_cols"]

    specify = None
    if base == "org_field":
        specify = _org_field
    elif base == "user_field":
        specify = _user_field
    elif base == "ticket":
        specify = _ticket
    elif base == "ticket_field":
        specify = _ticket_field

    spark = kwargs["spark"]
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

    # Remove old partitions
    _info(f"Deleting the {transform_path} in HDFS")
    delete_hdfs_path(spark, transform_path)
    _info(f"Path {transform_path} on HDFS successfully deleted ")

    # Transform

    _info(f"Writing {transform_path}...")

    raw_df.withColumn(
        "_transform_datetime", lit(str(datetime.utcnow())).cast("timestamp")
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
            if "Path does not exist" in str(e) or "Unable to infer schema" in str(e):
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
            old_df.write.mode("append").partitionBy(part_list).parquet(transform_path)

            _info(f"  Partitions appended to {transform_path} successfully.")


###############################################################################


def main():

    _info("Start")

    app_args = json.loads(job_args.zendesk_args)

    kwargs = {
        "spark": spark,
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
        "depara_path": app_args["depara_path"],
        "depara_fields_path": app_args["depara_fields_path"],
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
