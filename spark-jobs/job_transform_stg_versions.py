import os

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
        StructField("action", StringType(), True),
        StructField("old_value", StringType(), True),
        StructField("new_value", StringType(), True),
    ]
    return StructType(fields=fields)


def split_column_changes(string):
    list_string = string.replace("\n", "|").split("|")
    list_new_strings = []
    string = ""
    for i in list_string[1:-1]:
        if list_string.index(i) > 1:
            if (len(i) > 0) and (i[0] not in (" ", "'", "")):
                list_new_strings.append(string)
                string = ""
        string = string + i

    list_new_strings.append(string)

    list_dataframe = []

    for i in list_new_strings:
        if list_new_strings.index(i) % 3 == 0:
            list_values = [i.replace(":", "").strip()]
        elif list_new_strings.index(i) % 3 == 1:
            list_values.append(i[1:].replace("'", "").strip())
        elif list_new_strings.index(i) % 3 == 2:
            list_values.append(i[1:].replace("'", "").strip())
            list_dataframe.append(list_values)

    return list_dataframe


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    udf_split = f.udf(lambda x: split_column_changes(x), ArrayType(get_schema()))

    df_versions = spark.table(f"{jobExec.database_replica_full}.versions").filter(
        f.col("reference_date") == jobExec.reference_date
    )

    df_versions = df_versions.filter(
        (df_versions["item_type"].isin(["Gym", "Company", "Rec", "Person"]))
        & (f.col("object_changes").isNotNull())
    )

    # Explode column object_changes to create a new line for each item
    exploded_df = df_versions.select(
        "id",
        "item_type",
        "item_id",
        "event",
        "whodunnit",
        "created_at",
        "transaction_id",
        f.explode(udf_split("object_changes")).alias("exploded_data"),
    )

    # Creating the new columns
    df_stg_versions = (
        exploded_df.withColumn("action", f.col("exploded_data").getItem("action"))
        .withColumn(
            "old_value",
            f.substring(f.col("exploded_data").getItem("old_value"), 1, 100),
        )
        .withColumn(
            "new_value",
            f.substring(f.col("exploded_data").getItem("new_value"), 1, 100),
        )
        .drop("exploded_data")
    )

    df_stg_versions = dataframe_utils.createPartitionColumns(
        df_stg_versions, jobExec.reference_date
    )

    df_stg_versions = jobExec.select_dataframe_columns(
        spark, df_stg_versions, table_columns
    )
    df_stg_versions = df_stg_versions.repartition(num_partitions, "id")
    df_stg_versions.write.insertInto(
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
