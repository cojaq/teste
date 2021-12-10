import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
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

    list_actions = []
    actions = jobExec.selectFilterActions("GYM")

    for i in actions:
        list_actions.append(i[0])

    df_stg_versions = spark.table(f"{jobExec.database_work}.stg_versions").filter(
        (f.col("reference_date") == jobExec.reference_date)
        & (f.col("item_type").isin("Gym", "Rec"))
        & (f.upper(f.col("action")).isin(list_actions))
    )

    # This filter is used to not load the same data every execution
    df_gym_actions_history = spark.table(
        f"{jobExec.database_work}.gym_actions_history"
    ).filter(
        (f.col("date") < f.lit("2017-10-02"))
        & (f.col("reference_date") == jobExec.reference_date)
    )

    df_v_dim_gyms_actions = df_stg_versions.select(
        f.col("item_id").alias("gym_id"),
        f.col("created_at").alias("action_date"),
        "action",
        "old_value",
        "new_value",
    )

    df_gym_actions_history_status = df_gym_actions_history.select(
        f.col("id").alias("gym_id"),
        f.col("date").alias("action_date"),
        f.lit("status").alias("action"),
        f.lit(None).cast(StringType()).alias("old_value"),
        f.col("status").alias("new_value"),
    )

    df_gym_actions_history_enabled_1 = df_gym_actions_history.select(
        f.col("id").alias("gym_id"),
        f.col("date").alias("action_date"),
        f.lit("enabled_1").alias("action"),
        f.lit(None).cast(StringType()).alias("old_value"),
        f.col("enabled_1").alias("new_value"),
    )

    df_v_dim_gyms_actions = df_v_dim_gyms_actions.union(
        df_gym_actions_history_status
    ).union(df_gym_actions_history_enabled_1)

    df_v_dim_gyms_actions = dataframe_utils.createPartitionColumns(
        df_v_dim_gyms_actions, jobExec.reference_date
    )

    df_v_dim_gyms_actions = jobExec.select_dataframe_columns(
        spark, df_v_dim_gyms_actions, table_columns
    )
    df_v_dim_gyms_actions = df_v_dim_gyms_actions.repartition(num_partitions, "gym_id")

    df_v_dim_gyms_actions.write.insertInto(
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
