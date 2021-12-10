import os
from collections import Counter
from datetime import timedelta

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, IntegerType
from pyspark.sql.window import Window
from utils import arg_utils, dataframe_utils, date_utils

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

    reference_date_30_days = int(
        (
            date_utils.convert_text_to_date(jobExec.reference_date, "%Y%m%d")
            - timedelta(30)
        ).strftime("%Y%m%d")
    )

    initial_first_day_of_week = (
        spark.table(f"{jobExec.database_edw}.v_dim_date")
        .filter(f.col("id") == jobExec.reference_date)
        .agg(f.max(f.col("first_day_of_week")))
        .collect()[0][0]
    )

    # # Reading dates
    df_dates = (
        spark.table(f"{jobExec.database_edw}.v_dim_date")
        # .filter(f.col("first_day_of_week") == initial_first_day_of_week)
        .select("id", "first_day_of_week")
    )

    # df_person = (
    #     spark.table(f"{jobExec.database_edw}.v_dim_person")
    #     .select(
    #         "person_id",
    #         "company_id",
    #         "created_day",
    #         "disabled_day",
    #         "first_sign_in_day"
    #     )
    # )

    # df_companies = (
    #     spark.table(f"{jobExec.database_edw}.v_dim_companies")
    #     .select(
    #         "company_id",
    #         "parent_company_id",
    #         "self_registration_type",

    #         "sso_type",
    #         "country_id"
    #     )
    # )

    df_gyms = spark.table(f"{jobExec.database_replica_full}.gyms").select(
        "id", "personal_trainer"
    )

    df_transactions = (
        spark.table(f"{jobExec.database_edw}.v_dim_transactions")
        .filter(f.col("action").isin(["gym_visit", "gym_visit_retro"]))
        .select(
            "person_id",
            "company_id",
            "gym_id",
            "partner_visit_action",
            "date",
        )
    )

    windowSpecFirstVisit = (
        Window.partitionBy("person_id")
        .orderBy("first_day_of_week")
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_transactions_weekly = df_transactions.join(
        df_dates, [df_transactions["date"] == df_dates["id"]], "inner"
    ).select(
        "first_day_of_week",
        "person_id",
        "company_id",
        "gym_id",
        "partner_visit_action",
    )

    df_transactions_weekly = (
        df_transactions_weekly.withColumn(
            "first_visit",
            f.first("first_day_of_week").over(windowSpecFirstVisit),
        )
        .filter(f.col("first_day_of_week") >= initial_first_day_of_week)
        .select(
            "first_day_of_week",
            "first_visit",
            "person_id",
            "company_id",
            "gym_id",
            "partner_visit_action",
        )
    )

    df_transactions_weekly = (
        df_transactions_weekly.join(
            df_gyms,
            [df_transactions_weekly["gym_id"] == df_gyms["id"]],
            "left",
        )
        .withColumn(
            "type_visit",
            f.when(
                f.col("partner_visit_action") == "in_person_class",
                "in_person",
            )
            .when(
                (f.col("partner_visit_action") == "virtual_class")
                & (f.col("personal_trainer") == True),
                "personal_trainer",
            )
            .when(
                (f.col("partner_visit_action") == "virtual_class")
                & (f.col("personal_trainer") == False),
                "live_class",
            ),
        )
        .select(
            "first_day_of_week",
            "first_visit",
            "person_id",
            "company_id",
            "gym_id",
            "type_visit",
        )
    )

    # df_transactions_weekly.show()

    df_person = spark.table(f"{jobExec.database_replica_full}.people").select(
        "id", "unique_token"
    )

    df_person = spark.table(f"{jobExec.database_replica_full}.people").select(
        "id", "unique_token"
    )

    def get_most_common(l):
        return Counter(l).most_common(1)[0][0]

    get_most_common_pyspark = f.udf(lambda z: get_most_common(z))

    def most_common(l):
        return Counter(l).most_common(1)[0][0]

    most_common_pyspark = f.udf(lambda z: most_common(z))

    df_user_behavior_profile = df_transactions_weekly.groupBy(
        "first_day_of_week",
        df_transactions_weekly["person_id"],
        "company_id",
        "first_visit"
        # "country_title",
        # "country_id"
    ).agg(
        f.count("person_id").alias("visits_total"),
        f.count(f.when(f.col("type_visit") == "in_person", True)).alias(
            "visits_in_person"
        ),
        f.count(f.when(f.col("type_visit") == "personal_trainer", True)).alias(
            "visits_personal_trainer"
        ),
        f.count(f.when(f.col("type_visit") == "live_class", True)).alias(
            "visits_live_class"
        ),
        f.countDistinct("gym_id").alias("distinct_gyms_total"),
        get_most_common_pyspark(f.collect_list("gym_id")).alias("most_frequent_gym"),
    )

    df_user_behavior_profile = df_user_behavior_profile.join(
        df_person,
        [df_transactions_weekly["person_id"] == df_person["id"]],
        "left",
    )

    df_gw_usage = (
        spark.table(f"rabbitmq.wellness_partner_event_used")
        .filter(f.col("dt") >= initial_first_day_of_week)
        .withColumn(
            "wellness_id",
            f.when(f.length("gpw_id") > 1, f.col("gpw_id")).otherwise(f.col("email")),
        )
        .withColumn(
            "id_type",
            f.when(f.length("gpw_id") > 1, f.lit("gpw_id")).otherwise(f.lit("email")),
        )
        .select("dt", "wellness_id", "product_id", "id_type", "gpw_id", "email")
    )

    df_gw_user = (
        # spark.table(f"{jobExec.database_gympass_w}.user")
        spark.table(f"{jobExec.database_gympass_w}.user")
    )

    # Checkins from users using email as id
    df_gw_checkins_user_id = (
        df_gw_usage.filter(df_gw_usage["id_type"] == "gpw_id")
        .join(
            df_gw_user,
            df_gw_usage["wellness_id"] == df_gw_user["id"],
            "inner",
        )
        .select(
            "dt",
            "wellness_id",
            "product_id",
            f.col("g_id").alias("person_id"),
        )
        .distinct()
    )

    # Checkins from users using email as id
    df_gw_checkins_email = (
        df_gw_usage.filter(df_gw_usage["id_type"] == "email")
        .join(
            df_gw_user,
            df_gw_usage["wellness_id"] == df_gw_user["email"],
            "inner",
        )
        .select(
            "dt",
            "wellness_id",
            "product_id",
            f.col("g_id").alias("person_id"),
        )
        .distinct()
    )

    df_gw_chekins_total = df_gw_checkins_user_id.union(df_gw_checkins_email)

    df_gw_checkins_groupped = (
        df_gw_chekins_total.join(
            df_dates, df_gw_chekins_total["dt"] == df_dates["id"], "inner"
        )
        .groupBy("first_day_of_week", "person_id")
        .agg(
            f.countDistinct("dt").alias("w_checkins"),
            f.countDistinct("product_id").alias("distinct_w_apps"),
            get_most_common_pyspark(f.collect_list("product_id")).alias(
                "top_1_product_id"
            ),
        )
    )

    df_gw_product = spark.table(f"{jobExec.database_gympass_w}.product").select(
        "id", f.col("name").alias("gpw_app")
    )

    df_wellness = df_gw_checkins_groupped.join(
        df_gw_product,
        df_gw_checkins_groupped["top_1_product_id"] == df_gw_product["id"],
        "inner",
    ).select(
        "first_day_of_week",
        "person_id",
        "w_checkins",
        "distinct_w_apps",
        f.col("gpw_app").alias("most_frequent_w_app"),
    )

    df_user_behavior_profile = df_user_behavior_profile.join(
        df_wellness,
        [
            (
                df_user_behavior_profile["first_day_of_week"]
                == df_wellness["first_day_of_week"]
            )
            & (df_user_behavior_profile["person_id"] == df_wellness["person_id"])
        ],
        "outer",
    ).select(
        f.coalesce(
            df_user_behavior_profile["first_day_of_week"],
            df_wellness["first_day_of_week"],
        ).alias("first_day_of_week"),
        f.coalesce(
            df_user_behavior_profile["person_id"], df_wellness["person_id"]
        ).alias("person_id"),
        df_user_behavior_profile["unique_token"],
        "company_id",
        "first_visit",
        "visits_total",
        "visits_in_person",
        "visits_live_class",
        "visits_personal_trainer",
        "distinct_gyms_total",
        "most_frequent_gym",
        "w_checkins",
        "distinct_w_apps",
        "most_frequent_w_app",
        # "count_app_sessions"
    )

    # RAW APP SESSIONS
    df_app_sessions = (
        spark.table(f"{jobExec.database_web_analytics}.firebase_session_start")
        .filter(f.col("reference_date") >= initial_first_day_of_week)
        .select("reference_date", "user_id")
    )
    # JOIN APP SESSIONS AGGREGATED BY WEEK
    df_app_sessions = df_app_sessions.join(
        df_dates,
        [df_dates["id"] == df_app_sessions["reference_date"]],
        "inner",
    )
    # AGGREGATING APP SESSIONS
    df_app_sessions_weekly = (
        df_app_sessions.groupBy("first_day_of_week", "user_id")
        .agg(f.count("user_id").alias("count_app_sessions"))
        .select(
            "first_day_of_week",
            f.col("user_id").alias("unique_token"),
            "count_app_sessions",
        )
    )
    # JOIN WITH APP SESSIONS
    df_user_behavior_profile_final = df_user_behavior_profile.join(
        df_app_sessions_weekly,
        [
            (
                df_user_behavior_profile["unique_token"]
                == df_app_sessions_weekly["unique_token"]
            )
            & (
                df_user_behavior_profile["first_day_of_week"]
                == df_app_sessions_weekly["first_day_of_week"]
            )
        ],
        "left",
    ).select(
        df_user_behavior_profile["first_day_of_week"],
        df_user_behavior_profile["person_id"],
        df_user_behavior_profile["unique_token"],
        "company_id",
        "first_visit",
        "visits_total",
        "visits_in_person",
        "visits_live_class",
        "visits_personal_trainer",
        "distinct_gyms_total",
        "most_frequent_gym",
        "w_checkins",
        "distinct_w_apps",
        "most_frequent_w_app",
        "count_app_sessions",
    )

    df_user_behavior_profile_final = jobExec.select_dataframe_columns(
        spark, df_user_behavior_profile_final, table_columns
    )
    df_user_behavior_profile_final = df_user_behavior_profile_final.repartition(
        num_partitions, "first_day_of_week"
    )

    df_user_behavior_profile_final.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True)
