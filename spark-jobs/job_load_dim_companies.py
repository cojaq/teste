import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType
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

    # Reading source table
    df_companies = (
        spark.table(f"{jobExec.database_replica_full}.companies")
        .select(
            "id",
            "parent_company_id",
            "title",
            "country_id",
            "latitude",
            "longitude",
            "company_status",
            "company_type",
            "disable_b2b",
            "dependent_type",
            "contract_ended_at",
            "total_employees",
            "total_dependents",
            "expected_enrollment_percentage",
            "payroll_enabled",
            "billing_day",
            "launched_at",
            "considered_quantity",
            "registered_monthly_value",
            "high_usage_min_person_count",
            "high_usage_max_person_count",
            "high_usage_person_monthly_discount_value",
            "low_usage_min_person_count",
            "low_usage_max_person_count",
            "low_usage_person_monthly_discount_value",
            "deployment_manager_id",
            "relationship_manager_id",
            "relationship_title",
            "self_registration_type",
            "hr_portal_enabled",
            "hr_portal_enabled_at",
            "free_trial_days",
            "sso_type",
            "company_relationship_status",
            "contact_permission",
            "salesforce_id",
        )
        .filter("company_status between 73 and 100 or company_status == 120")
    )

    df_salesforce_account = spark.table(
        f"{jobExec.database_salesforce_full}.account"
    ).select(f.col("id").alias("salesforce_id"), "business_unit__c")

    df_salesforce_opportunity = (
        spark.table(f"{jobExec.database_salesforce_full}.opportunity")
        .filter(
            (f.col("iswon") == True)
            & (f.col("isdeleted") == False)
            & (f.col("self_checkout__c") == True)
        )
        .select(f.col("accountid").alias("salesforce_id_opp"))
        .distinct()
    )

    df_crm_companies_support = spark.table(
        f"{jobExec.database_ods}.crm_companies_support"
    )

    df_crm_companies_fm = spark.table(f"{jobExec.database_ods}.crm_companies_fm")

    df_crm_companies_contract_title = spark.table(
        f"{jobExec.database_ods}.crm_companies_contract_title"
    ).select("company_id", "contract_title")

    df_countries = spark.table(f"{jobExec.database_replica_full}.countries").select(
        f.col("id").alias("country_id"), "timezone"
    )

    # Transform
    df_dim_companies = (
        df_companies.join(df_countries, "country_id", "inner")
        .withColumn(
            "hr_portal_enabled_day_local",
            f.date_format(
                f.from_utc_timestamp("hr_portal_enabled_at", "timezone"),
                "yyyyMMdd",
            ).cast(IntegerType()),
        )
        .withColumnRenamed("free_trial_days", "child_company_free_trial_days")
        .withColumnRenamed("sso_type", "child_company_sso_type")
        .withColumnRenamed(
            "company_relationship_status", "child_company_relationship_status"
        )
        .withColumnRenamed("contact_permission", "child_company_contact_permission")
        .select(
            f.col("id").alias("company_id"),
            f.coalesce(f.col("parent_company_id"), f.col("id")).alias(
                "parent_company_id"
            ),
            "title",
            f.col("latitude").alias("child_latitude"),
            f.col("longitude").alias("child_longitude"),
            f.col("launched_at").alias("child_launched_at"),
            "company_status",
            f.col("company_type").alias("child_company_type"),
            "disable_b2b",
            "dependent_type",
            "country_id",
            "contract_ended_at",
            "self_registration_type",
            "hr_portal_enabled",
            "hr_portal_enabled_day_local",
            "hr_portal_enabled_at",
            f.col("relationship_title").alias("child_relationship_title"),
            "child_company_free_trial_days",
            "child_company_relationship_status",
            "child_company_sso_type",
            "child_company_contact_permission",
            "salesforce_id",
        )
        .join(
            df_companies.select(
                f.col("id").alias("parent_company_id"),
                f.col("company_type").alias("parent_company_type"),
                f.col("title").alias("parent_company_title"),
                f.col("country_id").alias("parent_country_id"),
                f.col("latitude").alias("parent_latitude"),
                f.col("longitude").alias("parent_longitude"),
                "total_dependents",
                f.col("total_employees").alias("parent_total_employees"),
                "expected_enrollment_percentage",
                "payroll_enabled",
                "billing_day",
                f.col("launched_at").alias("parent_launched_at"),
                "considered_quantity",
                "registered_monthly_value",
                "high_usage_min_person_count",
                "high_usage_max_person_count",
                "high_usage_person_monthly_discount_value",
                "low_usage_min_person_count",
                "low_usage_max_person_count",
                "low_usage_person_monthly_discount_value",
                "relationship_manager_id",
                "deployment_manager_id",
                "relationship_title",
                f.col("self_registration_type").alias("parent_self_registration_type"),
            ),
            "parent_company_id",
            "inner",
        )
        .join(
            df_crm_companies_support,
            ["parent_company_id", "country_id"],
            "left",
        )
        .join(df_crm_companies_contract_title, "company_id", "left")
        .join(
            df_crm_companies_fm.withColumn(
                "flag_crm_companies_fm", f.lit(1).cast(IntegerType())
            ),
            "company_id",
            "left",
        )
        .join(df_salesforce_account, "salesforce_id", "left")
        .join(
            df_salesforce_opportunity,
            df_salesforce_opportunity["salesforce_id_opp"]
            == df_companies["salesforce_id"],
            "left",
        )
        .withColumn(
            "expected_enrollment_percentage",
            f.when(
                f.col("expected_enrollment_percentage") > 1,
                f.col("expected_enrollment_percentage") / 100,
            ).otherwise(f.col("expected_enrollment_percentage")),
        )
        .withColumn(
            "launched_day",
            f.coalesce(
                (
                    f.date_format(
                        f.coalesce("parent_launched_at", "child_launched_at"),
                        "yyyyMMdd",
                    )
                ).cast(IntegerType()),
                df_crm_companies_support["launched_day"],
            ),
        )
        .withColumn(
            "contract_type",
            f.when(
                (f.col("considered_quantity") != 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") != 0)
                & (f.col("high_usage_max_person_count") != 0)
                & (f.col("high_usage_person_monthly_discount_value") != 0)
                & (f.col("low_usage_min_person_count") != 0)
                & (f.col("low_usage_max_person_count") != 0)
                & (f.col("low_usage_person_monthly_discount_value") != 0),
                f.lit("Flat"),
            )
            .when(
                (f.col("considered_quantity") == 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") == 0)
                & (f.col("high_usage_max_person_count") == 0)
                & (f.col("high_usage_person_monthly_discount_value") == 0)
                & (f.col("low_usage_min_person_count") == 0)
                & (f.col("low_usage_max_person_count") == 0)
                & (f.col("low_usage_person_monthly_discount_value") == 0),
                f.lit("Flat per eligible"),
            )
            .when(
                (f.col("considered_quantity") != 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") == 0)
                & (f.col("high_usage_max_person_count") == 0)
                & (f.col("high_usage_person_monthly_discount_value") == 0)
                & (f.col("low_usage_min_person_count") == 0)
                & (f.col("low_usage_max_person_count") != 0)
                & (f.col("low_usage_person_monthly_discount_value") != 0),
                f.lit("Copay 2"),
            )
            .when(
                (f.col("considered_quantity") != 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") == 0)
                & (f.col("high_usage_max_person_count") == 0)
                & (f.col("high_usage_person_monthly_discount_value") == 0)
                & (f.col("low_usage_min_person_count") != 0)
                & (f.col("low_usage_max_person_count") != 0)
                & (f.col("low_usage_person_monthly_discount_value") != 0),
                f.lit("Copay 3"),
            )
            .when(
                (f.col("considered_quantity") != 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") == 0)
                & (f.col("high_usage_max_person_count") != 0)
                & (f.col("high_usage_person_monthly_discount_value") != 0)
                & (f.col("low_usage_min_person_count") == 0)
                & (f.col("low_usage_max_person_count") == 0)
                & (f.col("low_usage_person_monthly_discount_value") == 0),
                f.lit("Copay 2 inv."),
            )
            .when(
                (f.col("considered_quantity") != 0)
                & (f.col("registered_monthly_value") != 0)
                & (f.col("high_usage_min_person_count") != 0)
                & (f.col("high_usage_max_person_count") != 0)
                & (f.col("high_usage_person_monthly_discount_value") != 0)
                & (f.col("low_usage_min_person_count") == 0)
                & (f.col("low_usage_max_person_count") == 0)
                & (f.col("low_usage_person_monthly_discount_value") == 0),
                f.lit("Copay 3 inv."),
            )
            .otherwise(f.lit(None).cast(StringType())),
        )
        .withColumn(
            "family_member",
            f.when(
                (f.col("parent_company_type").isin(42, 45))
                & (f.col("child_company_type") == 19),
                f.lit(1),
            )
            .when(
                (f.col("parent_company_type") == 50) & (f.col("dependent_type") == 10),
                f.lit(1),
            )
            .when(f.col("flag_crm_companies_fm").isNotNull(), f.lit(1))
            .otherwise(f.lit(0)),
        )
        .withColumn(
            "total_employees",
            f.when(
                (df_crm_companies_support["contract_ended_day"].isNotNull())
                & (df_crm_companies_support["total_employees"] > 0),
                df_crm_companies_support["total_employees"],
            ).otherwise(f.col("parent_total_employees")),
        )
        .withColumn(
            "bu",
            f.when((f.col("business_unit__c") == "SMB"), f.lit(2))
            .when(
                (f.col("child_company_type") >= 18)
                & (f.col("child_company_type") != 50),
                f.lit(1),
            )
            .when(
                (f.col("child_company_type") == 50)
                & (df_companies["disable_b2b"] == 0),
                f.lit(1),
            )
            .otherwise(f.lit(0)),
        )
        .withColumn(
            "contract_ended_day",
            f.coalesce(
                f.coalesce(
                    df_crm_companies_support["contract_ended_day"],
                    f.date_format(df_companies["contract_ended_at"], "yyyyMMdd").cast(
                        IntegerType()
                    ),
                ),
                f.lit(0),
            ),
        )
        .withColumn(
            "self_registration_type",
            f.when(
                f.col("self_registration_type") == 0,
                f.lit("0. Not available"),
            )
            .when(
                f.col("self_registration_type") == 10,
                f.lit(
                    "10. Available necessarily using national and functional single number"
                ),
            )
            .when(
                f.col("self_registration_type") == 20,
                f.lit("20. Available using single national number only"),
            )
            .when(
                f.col("self_registration_type") == 30,
                f.lit("30. Available with only company token"),
            )
            .when(
                f.col("self_registration_type") == 40,
                f.lit("40. Available using cpf and birthday"),
            )
            .when(
                f.col("self_registration_type") == 50,
                f.lit("50. Just email with company domain"),
            )
            .otherwise(f.col("self_registration_type").cast(StringType())),
        )
        .withColumn(
            "parent_self_registration_type",
            f.when(
                f.col("parent_self_registration_type") == 0,
                f.lit("0. Not available"),
            )
            .when(
                f.col("parent_self_registration_type") == 10,
                f.lit(
                    "10. Available necessarily using national and functional single number"
                ),
            )
            .when(
                f.col("parent_self_registration_type") == 20,
                f.lit("20. Available using single national number only"),
            )
            .when(
                f.col("parent_self_registration_type") == 30,
                f.lit("30. Available with only company token"),
            )
            .when(
                f.col("parent_self_registration_type") == 40,
                f.lit("40. Available using cpf and birthday"),
            )
            .when(
                f.col("parent_self_registration_type") == 50,
                f.lit("50. Just email with company domain"),
            )
            .otherwise(f.col("parent_self_registration_type").cast(StringType())),
        )
        .withColumn(
            "child_company_sso_type",
            f.when(f.col("child_company_sso_type") == 0, f.lit("0. Disabled"))
            .when(f.col("child_company_sso_type") == 10, f.lit("10. JWT"))
            .when(f.col("child_company_sso_type") == 20, f.lit("20. SAML"))
            .when(f.col("child_company_sso_type") == 30, f.lit("30. OAUTH"))
            .otherwise(f.col("child_company_sso_type").cast(StringType())),
        )
        .withColumn(
            "child_company_relationship_status",
            f.when(
                f.col("child_company_relationship_status") == 0,
                f.lit("0. Pre-launch"),
            )
            .when(
                f.col("child_company_relationship_status") == 5,
                f.lit("5. Launch"),
            )
            .when(
                f.col("child_company_relationship_status") == 10,
                f.lit("10. Post-launch"),
            )
            .when(
                f.col("child_company_relationship_status") == 20,
                f.lit("20. Expansion"),
            )
            .when(
                f.col("child_company_relationship_status") == 30,
                f.lit("30. Pre-renewal"),
            )
            .when(
                f.col("child_company_relationship_status") == 40,
                f.lit("40. Post_renewal"),
            )
            .when(
                f.col("child_company_relationship_status") == 50,
                f.lit("50. Retention"),
            )
            .otherwise(f.col("child_company_relationship_status").cast(StringType())),
        )
        .withColumn(
            "child_company_contact_permission",
            f.when(
                f.col("child_company_contact_permission") == 10,
                f.lit("10. Whitelist - Contact all"),
            )
            .when(
                f.col("child_company_contact_permission") == 20,
                f.lit("20. SU - Users who signed up"),
            )
            .when(
                f.col("child_company_contact_permission") == 30,
                f.lit("30. AM - Active users"),
            )
            .when(
                f.col("child_company_contact_permission") == 40,
                f.lit("40. Blacklist - Do not contact"),
            )
            .otherwise(f.col("child_company_contact_permission").cast(StringType())),
        )
        .withColumn(
            "self_checkout_flag",
            f.when(
                df_salesforce_opportunity["salesforce_id_opp"].isNotNull(),
                f.lit(1),
            ).otherwise(f.lit(0)),
        )
        .select(
            "company_id",
            f.regexp_replace("title", ";", "").alias("title"),
            "country_id",
            "parent_country_id",
            "parent_company_id",
            f.regexp_replace("parent_company_title", ";", "").alias(
                "parent_company_title"
            ),
            "child_latitude",
            "child_longitude",
            "parent_latitude",
            "parent_longitude",
            "total_employees",
            "total_dependents",
            f.col("expected_enrollment_percentage")
            .cast(DoubleType())
            .alias("expected_enrollment"),
            "payroll_enabled",
            "billing_day",
            "launched_day",
            "company_status",
            "relationship_manager_id",
            "deployment_manager_id",
            "contract_type",
            "bu",
            f.col("family_member").cast(BooleanType()).alias("family_member"),
            "contract_ended_day",
            "contract_title",
            "relationship_title",
            "parent_self_registration_type",
            "self_registration_type",
            "hr_portal_enabled",
            "hr_portal_enabled_day_local",
            "hr_portal_enabled_at",
            "child_relationship_title",
            f.col("child_company_free_trial_days").alias("free_trial_days"),
            f.col("child_company_sso_type").alias("sso_type"),
            f.col("child_company_relationship_status").alias(
                "company_relationship_status"
            ),
            f.col("child_company_contact_permission").alias("contact_permission"),
            "salesforce_id",
            "self_checkout_flag",
        )
    )

    df_dim_companies = jobExec.select_dataframe_columns(
        spark, df_dim_companies, table_columns
    )
    df_dim_companies = df_dim_companies.repartition(num_partitions, "company_id")

    df_dim_companies.write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    jobExec.totalLines = (
        spark.table(f"{jobExec.target_schema}.{jobExec.target_table}")
    ).count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
