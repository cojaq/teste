import os
import uuid
from datetime import datetime
from select import select
from time import strftime

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType
from pyspark.sql.window import Window
from utils import arg_utils, dataframe_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
jobExec = jobControl.Job(job_name, job_args)

###


def b2b_company_type(value):
    try:
        company_type = int(value)
        return True if company_type >= 19 else False
    except (ValueError, TypeError):
        return False


@f.udf
def platform(details):
    redirect_uri = details.get("redirect_uri")
    platform = "UNKNOWN"
    if redirect_uri is None:
        provider = details.get("identity_provider")
        if provider is not None:
            platform = provider
        else:
            platform = "NOT_DEFINED"
            user_agent = details.get("user-agent")
            if user_agent is None:
                user_agent = details.get("User-Agent")
            if user_agent is not None:
                user_agent = user_agent.upper()
                if "IPHONE" in user_agent:
                    platform = "IOS"
                elif "ANDROID" in user_agent:
                    platform = "ANDROID"
                else:
                    platform = "DESKTOP"
    elif "https://" in redirect_uri or "http://" in redirect_uri:
        platform = "DESKTOP"
    elif "gympass://gympass/" in redirect_uri:
        platform = "ANDROID"
    elif "gympass://" in redirect_uri:
        platform = "IOS"
    else:
        platform = "UNKNOWN"

    if platform == "DESKTOP" and (
        "app-version" in details
        or "x-device-id" in details
        or details.get("X-Requested-With") == "com.gympass"
    ):
        platform = "MOBILE_WEBVIEW"

    return platform


@f.udf
def user_type_column(id, company_type, disable_b2b):
    if id is None:
        return "NOT ASSOCIATED"
    elif b2b_company_type(company_type) and not bool(disable_b2b):
        return "B2B"
    return "B2C"


@f.udf
def company_type_column(company_type):
    if company_type == 42:
        return "subcompany"
    elif company_type == 19:
        return "dependent 19"
    elif company_type == 14:
        return "marketing"
    elif company_type == 40:
        return "kpmg"
    elif company_type == 50:
        return "standard"
    return company_type


@f.udf(returnType=DateType())
def parse_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")


@f.udf
def locked(people_locked):
    return people_locked is not None


@f.udf
def hr_manager(person_type_by_contacts_person_type, hr_id_user_id):
    return (
        person_type_by_contacts_person_type == "hr_manager" or hr_id_user_id is not None
    )


@f.udf
def gym_manager(person_type_by_contacts_person_type):
    return person_type_by_contacts_person_type == "gym_manager"


@f.udf
def b2b(company_members_companies_company_type, hr_id_user_id):
    return b2b_company_type(company_members_companies_company_type) and not bool(
        hr_id_user_id
    )


@f.udf
def email_confirmed(people_confirmed_at):
    return "false" if people_confirmed_at is None else "true"


@f.udf
def new_uuid():
    return str(uuid.uuid4())


target_columns = [
    "user_id",
    "fid",
    "client_id",
    "username",
    "previous_login_event_time",
    "login_event_time",
    "last_operation_event_time",
    "last_operation_event_type",
    "session_id",
    "last_login_interval_min",
    "last_operation_interval_min",
    "user_agent",
    "device_platform",
    "event_time",
    "event_id",
    "blocked",
    "locked",
    "hr_manager",
    "gym_manager",
    "b2b",
    "family_member",
    "user_type",
    "user_status",
    "user_plan",
    "email_confirmed",
    "confirmed_at",
    "company_type",
    "last_check_in_event_time",
    "dt",
]

###


def loginFilters(df_identity, num_partitions):
    df_identity_select = df_identity.select(
        f.split(f.col("user_id"), ":")[2].alias("user_id"),
        f.split(f.col("user_id"), ":")[1].alias("fid"),
        "client_id",
        f.col("keycloak_type").alias("last_operation_event_type"),
        f.col("session_id").alias("session_id"),
        f.when(
            f.col("details").getItem("User-Agent").isNull(),
            f.col("details").getItem("user-agent"),
        )
        .otherwise(f.col("details").getItem("User-Agent"))
        .alias("user_agent"),
        platform(f.col("details")).alias("device_platform"),
        f.col("_event_time").alias("event_time"),
        "dt",
    )

    df_identity.unpersist(True)

    w_session = Window.partitionBy("session_id").orderBy(f.col("event_time").asc())
    w_login = Window.partitionBy(["user_id", "client_id", "user_agent"]).orderBy(
        f.col("event_time").asc()
    )

    df_identity = (
        df_identity_select.filter(f.col("user_id").isNotNull())
        .filter(f.col("fid").isNotNull())
        .withColumn("min_time", f.min("event_time").over(w_session))
        .where(f.col("event_time") == f.col("min_time"))
        .drop("min_time")
        .withColumn("login_event_time", f.col("event_time"))
        .withColumn(
            "previous_login_event_time",
            f.lag(f.col("event_time")).over(w_login),
        )
        .withColumn(
            "last_login_interval_min",
            (f.col("event_time") - f.col("previous_login_event_time")) / (1000 * 60),
        )
        .withColumn("event_id", new_uuid())
    )

    df_identity_select.unpersist(True)

    return df_identity.repartition(num_partitions, "user_id", "client_id")


def addLastKeycloakEvent(df_identity, source_table_columns):
    df_dim_identity_temp = (
        spark.table(f"{jobExec.source_schema}.{jobExec.source_table}")
        .filter((f.col("dt") >= jobExec.reference_date))
        .filter((f.col("dt") < datetime.today().strftime("%Y%m%d")))
        .filter(f.col("keycloak_type") != "LOGIN")
    )

    df_dim_identity = jobExec.select_dataframe_columns(
        spark, df_dim_identity_temp, source_table_columns
    )

    df_dim_identity_temp.unpersist(True)

    count_lines = df_identity.count()
    jobExec.totalLines = count_lines
    num_partitions = jobExec.calcNumPartitions()

    df_dim_identity_temp = df_dim_identity.repartition(
        num_partitions, "user_id", "client_id"
    ).select(
        f.col("user_id").alias("dim_user_id"),
        f.col("client_id").alias("dim_client_id"),
        f.col("session_id").alias("dim_session_id"),
        f.col("keycloak_type").alias("dim_keycloak_type"),
        f.col("_event_time").alias("dim_event_time"),
    )

    w_session = Window.partitionBy("dim_session_id").orderBy(
        f.col("dim_event_time").desc()
    )

    df_dim_identity.unpersist(True)

    df_dim_identity = (
        df_dim_identity_temp.withColumn(
            "max_time", f.max("dim_event_time").over(w_session)
        )
        .where(f.col("dim_event_time") == f.col("max_time"))
        .drop("max_time")
    )

    df_dim_identity_temp.unpersist(True)

    df_ev_identity = (
        df_identity.join(
            df_dim_identity,
            on=df_identity["session_id"] == df_dim_identity["dim_session_id"],
            how="inner",
        )
        .withColumn("last_operation_event_time", f.col("dim_event_time"))
        .withColumn("last_operation_event_type", f.col("dim_keycloak_type"))
        .withColumn(
            "last_operation_interval_min",
            (f.col("last_operation_event_time") - f.col("event_time")) / (1000 * 60),
        )
    )

    df_ev_identity = df_ev_identity.drop(
        "dim_user_id",
        "dim_client_id",
        "dim_session_id",
        "dim_keycloak_type",
        "dim_event_time",
    )

    df_identity.unpersist(True)
    df_dim_identity.unpersist(True)

    return df_ev_identity.repartition(num_partitions, "dt", "user_id", "client_id")


def addCorePerson(df_identity):
    df_person = spark.table(f"{jobExec.database_replica_full}.people").select(
        f.col("id").alias("people_id"),
        f.col("unique_token").alias("people_unique_token"),
        f.col("name").alias("people_name"),
        f.col("confirmed_at").alias("people_confirmed_at"),
        f.col("email").alias("username"),
        f.col("email").alias("people_email"),
        f.col("blocked").alias("people_blocked"),
        f.col("locked_at").alias("people_locked"),
    )
    df_identity_result = df_identity.join(
        df_person, [df_identity["user_id"] == df_person["people_id"]], "left"
    )

    df_person.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addCoreCompanies(df_identity):
    df_companies = spark.table(f"{jobExec.database_replica_full}.companies").select(
        f.col("id").alias("companies_id"),
        f.col("payment_person_id").alias("companies_payment_person_id"),
    )
    df_identity_result = (
        df_identity.join(
            df_companies,
            [df_identity["people_id"] == df_companies["companies_payment_person_id"]],
            "left",
        )
    ).withColumn("hr_id_user_id", df_companies["companies_id"])

    df_companies.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addCoreCompanyMembers(df_identity):
    df_company_members = (
        spark.table(f"{jobExec.database_replica_full}.company_members")
        .select(
            f.col("person_id").alias("company_members_person_id"),
            f.col("company_id").alias("company_members_payment_company_id"),
            f.col("enabled").alias("company_members_enabled"),
        )
        .filter(f.col("company_members_enabled") == True)
        .distinct()
    )

    df_identity_temp = df_identity.join(
        df_company_members,
        [df_identity["people_id"] == df_company_members["company_members_person_id"]],
        "left",
    )

    df_company_members.unpersist(True)
    df_identity.unpersist(True)

    # join with companies
    df_company_members_companies = spark.table(
        f"{jobExec.database_replica_full}.companies"
    ).select(
        f.col("id").alias("company_members_companies_id"),
        f.col("company_type").alias("company_members_companies_company_type"),
        f.col("disable_b2b").alias("company_members_companies_company_disable_b2b"),
    )

    df_identity = df_identity_temp.join(
        df_company_members_companies,
        [
            df_identity_temp["company_members_payment_company_id"]
            == df_company_members_companies["company_members_companies_id"]
        ],
        "left",
    )

    df_company_members_companies.unpersist(True)
    df_identity_temp.unpersist(True)

    return df_identity


def addPersonTypeByContacts(df_identity):
    df_emails = spark.table(f"{jobExec.database_replica_full}.emails").select(
        f.col("emailable_id").alias("emails_emailable_id"),
        f.col("emailable_type").alias("emails_emailable_type"),
        f.col("email_address").alias("emails_email_address"),
    )

    df_emails_contacts_gym_manager_1 = (
        df_emails.filter((f.col("emails_emailable_type") == "Gym"))
        .select(
            f.col("emails_email_address").alias("person_type_by_contacts_email"),
            f.lit("gym_manager").alias("person_type_by_contacts_person_type"),
        )
        .distinct()
    )
    df_emails_contacts_rh_manager_0 = (
        df_emails.filter((f.col("emails_emailable_type") == "Company"))
        .select(
            f.col("emails_email_address").alias("person_type_by_contacts_email"),
            f.lit("hr_manager").alias("person_type_by_contacts_person_type"),
        )
        .distinct()
    )

    df_person_type_by_email = df_emails_contacts_gym_manager_1.union(
        df_emails_contacts_rh_manager_0
    ).distinct()

    df_identity_result = df_identity.join(
        df_person_type_by_email,
        [
            df_identity["people_email"]
            == df_person_type_by_email["person_type_by_contacts_email"]
        ],
        "left",
    )

    df_emails_contacts_gym_manager_1.unpersist(True)
    df_emails_contacts_rh_manager_0.unpersist(True)
    df_emails.unpersist(True)
    df_person_type_by_email.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addFamilyMembers(df_identity):
    df_v_dim_person = spark.table(f"{jobExec.database_edw}.v_dim_person").select(
        f.col("disabled_at_local").alias("v_dim_person_disabled_at_local"),
        f.col("company_id").alias("v_dim_person_company_id"),
        f.col("person_id").alias("v_dim_person_person_id"),
        f.col("family_member").alias("v_dim_person_family_member"),
        f.col("status").alias("v_dim_person_status"),
    )

    df_identity_result = (
        df_identity.join(
            df_v_dim_person,
            [
                df_identity["people_id"] == df_v_dim_person["v_dim_person_person_id"],
                df_identity["company_members_companies_id"]
                == df_v_dim_person["v_dim_person_company_id"],
            ],
            "left",
        ).filter(f.col("v_dim_person_disabled_at_local").isNull())
    ).distinct()

    df_v_dim_person.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addLastCheckIn(df_identity):
    df_transactions = (
        spark.table(f"{jobExec.database_edw}.v_dim_transactions")
        .filter(f.col("action").isin(["gym_visit", "gym_visit_retro"]))
        .groupBy("person_id")
        .agg(f.max("date").alias("max_v_dim_transactions_date"))
        .select(
            f.col("person_id").alias("v_dim_transactions_person_id"),
            f.col("max_v_dim_transactions_date"),
        )
    )

    df_identity_result = (
        df_identity.join(
            df_transactions,
            [
                df_identity["people_id"]
                == df_transactions["v_dim_transactions_person_id"]
            ],
            "left",
        )
    ).distinct()

    df_transactions.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addCorePersonProducts(df_identity):
    df_person_products = (
        spark.table(f"{jobExec.database_replica_full}.person_products")
        .filter(f.col("paid_day").isNotNull())
        .select(
            f.col("person_id").alias("person_products_person_id"),
            f.col("monthly_value").alias("person_products_monthly_value"),
            f.col("valid_start_date").alias("person_products_valid_start_date"),
            f.col("valid_end_date").alias("person_products_valid_end_date"),
            f.col("paid_day").alias("person_products_paid_day"),
        )
    )
    df_identity_result = (
        (
            df_identity.join(
                df_person_products,
                [
                    df_identity["user_id"]
                    == df_person_products["person_products_person_id"]
                ],
                "left",
            )
        )
        .filter(
            parse_date(df_identity["dt"]).between(
                f.col("person_products_valid_start_date"),
                f.col("person_products_valid_end_date"),
            )
        )
        .distinct()
    )

    df_person_products.unpersist(True)
    df_identity.unpersist(True)

    return df_identity_result


def addCoreInfos(df_identity):
    num_partitions = jobExec.calcNumPartitions()

    # allBasicEntities
    df_identity = addCorePerson(df_identity)
    df_identity = addCoreCompanies(df_identity)
    df_identity = addCoreCompanyMembers(df_identity)
    df_identity = addPersonTypeByContacts(df_identity)
    df_identity = addFamilyMembers(df_identity)
    df_identity = addLastCheckIn(df_identity)
    df_identity = addCorePersonProducts(df_identity)

    ###
    df_identity = (
        df_identity.withColumn("blocked", f.col("people_blocked"))
        .withColumn("locked", locked(f.col("people_locked")))
        .withColumn(
            "hr_manager",
            hr_manager(
                f.col("person_type_by_contacts_person_type"),
                f.col("hr_id_user_id"),
            ),
        )
        .withColumn(
            "gym_manager",
            gym_manager(f.col("person_type_by_contacts_person_type")),
        )
        .withColumn(
            "b2b",
            b2b(
                f.col("company_members_companies_company_type"),
                f.col("company_members_companies_company_disable_b2b"),
            ),
        )
        .withColumn("family_member", f.col("v_dim_person_family_member"))
        .withColumn(
            "user_type",
            user_type_column(
                f.col("company_members_companies_id"),
                f.col("company_members_companies_company_type"),
                f.col("company_members_companies_company_disable_b2b"),
            ),
        )
        .withColumn("user_status", f.col("v_dim_person_status"))
        .withColumn("user_plan", f.col("person_products_monthly_value"))
        .withColumn("email_confirmed", email_confirmed(f.col("people_confirmed_at")))
        .withColumn("confirmed_at", f.col("people_confirmed_at"))
        .withColumn(
            "company_type",
            company_type_column(f.col("company_members_companies_company_type")),
        )
        .withColumn("last_check_in_event_time", f.col("max_v_dim_transactions_date"))
    )
    ##

    df_identity = df_identity.select(*target_columns).distinct()

    return df_identity.repartition(num_partitions, "dt", "user_id", "client_id")


def main():
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    source_table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.source_schema, jobExec.source_table
    )
    df_identity = (
        spark.table(f"{jobExec.source_schema}.{jobExec.source_table}")
        .filter((f.col("dt") >= jobExec.reference_date))
        .filter((f.col("dt") < datetime.today().strftime("%Y%m%d")))
        .filter(f.col("keycloak_type") == "LOGIN")
    )

    df_identity = jobExec.select_dataframe_columns(
        spark, df_identity, source_table_columns
    )

    count_lines = df_identity.count()

    jobExec.totalLines = count_lines
    num_partitions = jobExec.calcNumPartitions()

    if jobExec.totalLines == 0:
        jobExec.logger.warning("Skipping extract job because query return zero results")
    else:
        df_identity = loginFilters(df_identity, num_partitions)
        df_identity = addLastKeycloakEvent(df_identity, source_table_columns)
        df_identity = addCoreInfos(df_identity)

        df_identity.write.insertInto(
            f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
        )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True)
