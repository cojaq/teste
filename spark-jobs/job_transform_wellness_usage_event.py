import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = jobExec.target_schema if jobExec.target_schema else "analytics"

num_partitions = 2


def main():
    # *************************************************************************** #
    query_company_members = """
        SELECT
            CAST(person_id AS string) AS person_id,
            CAST(company_id AS string) AS company_id,
            country_code
        FROM (
            SELECT
                COALESCE (cm.person_id, pe.id) AS person_id,
                company_id,
                c.short_title AS country_code,
                ROW_NUMBER() OVER (PARTITION BY COALESCE (cm.person_id, pe.id) ORDER BY cm.created_at DESC) AS rank_filter
            FROM replica_full.people pe
            LEFT JOIN replica_full.company_members cm ON pe.id = cm.person_id
            JOIN replica_full.countries c ON COALESCE(cm.country_id, pe.country_id) = c.id
        )
        WHERE rank_filter = 1
    """
    df_members = spark.sql(query_company_members)
    df_members.createOrReplaceTempView("company_members")
    df_members.cache()

    # *********************************************************************** #
    query_person_products = """
        SELECT
            cast(p.id AS string) AS plan_purchase_id,
            date(p.valid_start_date) AS valid_start_date,
            date(p.valid_end_date) AS valid_end_date,
            CASE 
                WHEN p.refunded_at < p.valid_end_date THEN date(p.refunded_at)
                ELSE date(p.valid_end_date)
            END AS paid_at,
            date(p.refunded_at) AS refunded_at,
            CAST(p.company_id AS string) AS client_id,
            p.parent_company_id,
            cast(p.person_id AS string) AS user_id,
            p.unit_price AS plan_monthly_value,
            c.short_title AS plan_currency,
            TRUE AS plan_active,
            co.short_title AS country_code
        FROM replica_full.person_products p
        INNER JOIN replica_full.currencies c 
            ON p.currency_id = c.id 
        INNER JOIN replica_full.countries co
            ON p.country_id = co.id    
        WHERE 
            TRUE 
            AND p.product_id IN (17,18,21,23,24,29)
            AND p.paid_at IS NOT NULL
            AND p.valid_start_date > date('2020-01-01')
            AND p.valid_start_date > date_sub(current_date, 120)
            AND p.report_id != 16
    """
    df_person_products = spark.sql(query_person_products)
    df_person_products.createOrReplaceTempView("person_products")
    df_person_products.cache()

    # *********************************************************************** #
    query_category = """
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
    df_category = spark.sql(query_category)
    df_category.createOrReplaceTempView("product_has_category")
    df_category.cache()

    # *********************************************************************** #
    query_tagus_transactions = """
        SELECT
            tr.id AS plan_purchase_id,
            tr.shopper_reference as user_id,
            uo.client_id,
            pl.standard_monthly_price AS plan_monthly_value,
            tr.amount AS amount_paid,
            tr.currency AS plan_currency,
            tr.event_time AS paid_at,
            date(tr.event_time) AS valid_start_day,
            date_add(add_months(tr.event_time, 1), -1) AS valid_end_day,
            TRUE AS plan_active,
            co.country_id AS country_code,
            ROW_NUMBER () OVER (PARTITION BY tr.shopper_reference, uo.client_id ORDER BY tr.event_time) AS subscription_sequence
        FROM transaction_manager.transactions tr
        INNER JOIN transaction_manager.transaction_status ts
            ON tr.id = ts.transaction_id AND ts.status IN ('SUCCEEDED','CREATED')
        INNER JOIN tagus.countries co
            ON tr.country_code = co.country_id
        LEFT JOIN tagus.plans pl
            ON CAST(get_json_object(tr.additional_info,'$.plan_id') AS string) = pl.plan_id
        LEFT JOIN tagus.user_orders_v2 uo
            ON CAST(get_json_object(tr.additional_info,'$.user_order_id') AS string) = uo.user_order_id
        WHERE
            UPPER(tr.event_type) = 'PURCHASE'
    """
    df_tagus_transactions = spark.sql(query_tagus_transactions)
    df_tagus_transactions.createOrReplaceTempView("tagus_transactions")

    # *********************************************************************** #
    query_wellness_event = """
        SELECT
            e._event_id AS event_id,
            from_unixtime(e._event_time/1000, 'yyyy-MM-dd HH:mm:ss') AS considered_at,
            pr.id AS product_id,
            CASE WHEN length(trim(e.email)) < 1 THEN NULL ELSE lower(trim(e.email)) END AS email,
            CASE WHEN length(trim(e.event_type)) < 1 THEN NULL ELSE trim(e.event_type) END AS usage_type,
            to_timestamp(e.timestamp, "y-MM-dd'T'HH:mm:ss'Z'") AS event_time,
            CASE WHEN length(trim(e.gpw_id)) != 40 THEN NULL ELSE trim(e.gpw_id) END AS gpw_id,
            CASE WHEN length(trim(e.user_id)) < 1 THEN NULL ELSE trim(e.user_id) END AS user_id,
            e.event_duration,
            e.viewing_duration,
            e.dt,
            pr.slug AS app_slug,
            pr.gym_id
        FROM kafka.wellness_event_publisher_wellness_partner_event_used e
        INNER JOIN gympass_w.product pr 
            ON e.product_id = pr.id
        WHERE
            to_date(e.dt,'yyyyMMdd') > date_sub(current_date, 90)
    """
    df_wellness_event = spark.sql(query_wellness_event)
    df_wellness_event.createOrReplaceTempView("wellness_partner_event_used_full")
    df_wellness_event.cache()
    # *********************************************************************** #

    query_usage = """
        WITH 
        wellness_partner_event_used AS (
            SELECT 
                e.event_id,
                e.event_time,
                e.product_id,
                e.gym_id,
                e.app_slug,
                e.usage_type,
                e.event_duration,
                e.viewing_duration,
                e.considered_at,
                e.dt,
                u.g_id AS user_id,
                CASE LENGTH(TRIM(u.g_id)) WHEN 36 THEN 'tagus' ELSE 'core' END AS user_id_source,
                trim(u.id) AS gpw_id
            FROM wellness_partner_event_used_full e 
            INNER JOIN gympass_w.user u 
                ON e.gpw_id = u.id
            UNION ALL
            SELECT 
                e.event_id,
                e.event_time,
                e.product_id,
                e.gym_id,
                e.app_slug,
                e.usage_type,
                e.event_duration,
                e.viewing_duration,
                e.considered_at,
                e.dt,
                u.g_id AS user_id,
                CASE LENGTH(TRIM(u.g_id)) WHEN 36 THEN 'tagus' ELSE 'core' END AS user_id_source,
                trim(u.id) AS gpw_id
            FROM wellness_partner_event_used_full e 
            INNER JOIN gympass_w.user u 
                ON LOWER(e.email) = LOWER(TRIM(u.email)) AND e.gpw_id IS NULL
        ),
        wellness_usage AS (
            SELECT 
                e.event_id,
                e.event_time AS usage_received_at,
                e.user_id,
                e.user_id_source,
                e.gpw_id,
                e.product_id,
                e.gym_id,
                e.app_slug,
                e.usage_type,
                c.category_name,
                c.fitness,
                e.event_duration,
                e.viewing_duration,
                e.considered_at,
                e.dt
            FROM wellness_partner_event_used e
            LEFT JOIN product_has_category c 
                ON e.product_id = c.product_id
        )
        SELECT 
            wu.event_id,
            wu.usage_received_at,
            wu.user_id,
            wu.user_id_source,
            wu.gpw_id,
            wu.product_id,
            wu.gym_id,
            wu.app_slug,
            wu.usage_type,
            wu.category_name,
            wu.fitness,
            wu.event_duration,
            wu.viewing_duration,
            wu.considered_at,
            wu.dt,
            upper(COALESCE(tu.country_id, pp.country_code, cm.country_code)) AS country_code,
            COALESCE(pp.client_id, cm.company_id, tt.client_id) AS client_id,
            COALESCE(pp.plan_purchase_id, tt.plan_purchase_id) AS plan_purchase_id,
            COALESCE(pp.plan_active, tt.plan_active) AS plan_active,
            COALESCE(pp.plan_monthly_value, tt.plan_monthly_value) AS plan_monthly_value,
            COALESCE(pp.plan_currency, tt.plan_currency) AS plan_currency,
            COALESCE(pp.valid_start_date, tt.valid_start_day) AS valid_start_date,
            row_number() OVER (PARTITION BY wu.event_id ORDER BY COALESCE(pp.valid_start_date, tt.valid_start_day) DESC) AS rank_filter
        FROM wellness_usage wu
        LEFT JOIN tagus.users tu 
            ON wu.user_id = tu.user_id AND wu.user_id_source = 'tagus'
        LEFT JOIN person_products pp 
            ON wu.user_id = pp.user_id AND wu.user_id_source = 'core'
            AND wu.dt BETWEEN date_format(pp.valid_start_date, 'yyyyMMdd') AND date_format(pp.paid_at, 'yyyyMMdd')
        LEFT JOIN company_members cm
            ON wu.user_id = cm.person_id AND pp.client_id IS NULL AND wu.user_id_source = 'core'
        LEFT JOIN tagus_transactions tt
            ON wu.user_id = tt.user_id AND wu.user_id_source = 'tagus'
            AND wu.dt BETWEEN date_format(tt.valid_start_day, 'yyyyMMdd') AND date_format(tt.valid_end_day, 'yyyyMMdd')
    """
    # *********************************************************************** #

    wellness_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    df = (
        spark.sql(query_usage)
        .select(wellness_columns)
        .filter(f.col("rank_filter") == "1")
    )

    jobExec.totalLines = df.count()

    # write dataframe
    if jobExec.totalLines > 0:
        # save to datalake
        df.repartition(num_partitions, "dt").write.insertInto(
            f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
        )
    else:
        jobExec.logger.warning(f"Extraction to {jobExec.target_table} is empty")


# *************************************************************************** #

if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=False)
