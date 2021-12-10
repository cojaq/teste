import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 6

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = jobExec.target_schema if jobExec.target_schema else "analytics"


def main():
    table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    query_w_category = """
        SELECT 
            product_id,
            category_id,
            category_name,
            fitness
        FROM (
            SELECT 
                phc.product_id, 
                phc.category_id,
                replace(c.name, 'MAINCATEGORY.') AS category_name,
                CASE 
                    WHEN c.name RLIKE 'MIND|HEALTH|FAMILY' THEN 'Non-Fitness'
                    ELSE 'Fitness'
                END AS fitness,
                ROW_NUMBER () OVER (PARTITION BY phc.product_id ORDER BY replace(c.name, 'MAINCATEGORY.') ) AS rank_filter
            FROM gympass_w.product_has_category phc
            INNER JOIN gympass_w.category c
                ON phc.category_id = c.id
        )
        WHERE rank_filter = 1
    """
    df_w_category = spark.sql(query_w_category)
    df_w_category.createOrReplaceTempView("product_has_category")
    df_w_category.cache()

    # *********************************************************************** #

    query_wellness_event = """
        SELECT
            e.dt,
            from_unixtime(e._event_time/1000, 'yyyy-MM-dd HH:mm:ss') AS considered_at,
            CASE WHEN length(trim(e.gpw_id)) != 40 THEN NULL ELSE trim(e.gpw_id) END AS gpw_id,
            CASE WHEN length(trim(e.email)) < 1 THEN NULL ELSE lower(trim(e.email)) END AS email,
            pr.id AS product_id,
            pc.category_name AS app_category,
            pr.slug AS app_slug
        FROM kafka.wellness_event_publisher_wellness_partner_event_used e
        INNER JOIN gympass_w.product pr 
            ON e.product_id = pr.id
        INNER JOIN product_has_category pc
            ON pr.id = pc.product_id        
    """
    df_wellness_event = spark.sql(query_wellness_event)
    df_wellness_event.createOrReplaceTempView("wellness_partner_event_used_full")
    df_wellness_event.cache()

    # *********************************************************************** #

    query_wellness_checkin = """
        SELECT 
            e.dt,
            u.g_id AS person_id,
            p.country_id,
            e.product_id AS partner_id,
            'wellness_checkin' AS partner_visit_action,
            'digital' AS checkin_type,
            e.app_category AS category,
            'wellness_event_publisher_wellness_partner_event_used' AS data_source
        FROM wellness_partner_event_used_full e 
        INNER JOIN gympass_w.user u 
            ON e.gpw_id = u.id
        LEFT JOIN replica_full.people p
            ON u.g_id = CAST(p.id AS string)
        UNION
        SELECT 
            e.dt,
            u.g_id AS person_id,
            p.country_id,
            e.product_id AS partner_id,
            'wellness_checkin' AS partner_visit_action,
            'digital' AS checkin_type,
            e.app_category AS category,
            'wellness_event_publisher_wellness_partner_event_used' AS data_source
        FROM wellness_partner_event_used_full e 
        INNER JOIN gympass_w.user u 
            ON LOWER(e.email) = LOWER(TRIM(u.email)) AND e.gpw_id IS NULL
        LEFT JOIN replica_full.people p
            ON u.g_id = CAST(p.id AS string)
    """
    df_checkin_wellness = spark.sql(query_wellness_checkin)

    # *********************************************************************** #

    query_gym_visit = """
        SELECT 
            /*CAST(tr.id AS string) AS attendance_id, -- TBD*/
            CAST(tr.considered_day AS string) AS dt,
            CAST(tr.person_id AS string) AS person_id,
            tr.country_id,
            CAST(tr.gym_id AS string) AS partner_id,
            CASE 
                WHEN tr.product_id = 32 AND g.personal_trainer = TRUE THEN 'personal_trainer'
                WHEN tr.product_id = 32 AND g.personal_trainer = FALSE THEN 'live_class'
                ELSE 'in_person_class'
            END AS partner_visit_action,
            CASE
                WHEN tr.product_id = 16 THEN 'in_person'
                WHEN tr.product_id = 32 THEN 'digital' 
            END AS checkin_type,
            'Fitness' AS category,
            'old_core'  AS data_source
        FROM replica_full.person_transactions tr
        INNER JOIN replica_full.gyms g
            ON tr.gym_id = g.id
        WHERE
            TRUE
            AND tr.refunded_at IS NULL
            AND tr.person_id IS NOT NULL
            AND tr.product_id IN (16, 32)
            /*user used for bonus payment*/
            AND tr.person_id != 40881 AND tr.gym_id != 1
            AND tr.considered_day BETWEEN 20190101 AND CAST(date_format(current_date(), 'yMMdd') AS integer)
    """
    df_checkin_core = spark.sql(query_gym_visit)

    # *********************************************************************** #

    query_tagus = """
        WITH alfred AS (
            SELECT 
                considered_at,
                user_uid AS user_id,
                cast(get_json_object(details,'$.partner.legacy_id') AS integer) AS partner_id,
                cast(get_json_object(details,'$.product.is_live_class') AS string) AS is_live_class,
                cast(get_json_object(details,'$.product.name') AS string) AS product_name,
                type_id
            FROM alfred.attendances a
            LEFT JOIN alfred.attendance_details d 
                ON a.id = d.id
        )
        SELECT 
            cast(date_format(considered_at, 'yMMdd') AS string) AS dt,
            cast(user_id AS string) AS person_id,
            rg.country_id AS country_id,
            cast(partner_id AS string) AS partner_id,
            CASE
                WHEN at.value = 'live-class' THEN 'live_class'
                WHEN is_personal_trainer = true THEN 'personal_trainer'
                ELSE 'in_person_class'
            END AS partner_visit_action,
            CASE
                WHEN at.value = 'live-class' or is_personal_trainer = true THEN 'digital'
                ELSE 'in_person_class'
            END AS checkin_type,
            'Fitness' AS category,
            'tagus'  AS data_source
        FROM alfred t
        LEFT JOIN booking.gyms g
            ON t.partner_id = g.id
        LEFT JOIN replica_full.gyms rg
            ON t.partner_id = rg.id
        LEFT JOIN alfred.attendance_types at 
            ON t.type_id = at.id
    """
    df_checkin_tagus = spark.sql(query_tagus)

    # *********************************************************************** #

    df_checkin = df_checkin_core.union(df_checkin_tagus).union(df_checkin_wellness)
    # df_checkin = jobExec.select_dataframe_columns(spark, df_checkin, table_columns)

    df_checkin = df_checkin.repartition(num_partitions, "dt", "partner_visit_action")

    df_checkin.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = df_checkin.count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
