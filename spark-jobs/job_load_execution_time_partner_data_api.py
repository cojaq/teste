from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from utils import arg_utils, database_utils, s3_utils

job_args = arg_utils.get_job_args()
job_name = f"job_load_execution_time_partner_data_api"

partner_data_api_host = job_args.database_host
partner_data_api_user = job_args.database_user
partner_data_api_password = job_args.database_password

jobExec = jobControl.Job(job_name, job_args)

target_connection_properties = {
    "database": "partner_data_api",
    "hostname": partner_data_api_host,
    "port": 5432,
    "username": partner_data_api_user,
    "password": partner_data_api_password,
}


target_spark_properties = {
    "user": partner_data_api_user,
    "password": partner_data_api_password,
    "driver": "org.postgresql.Driver",
}


def main():
    with database_utils.PostgreSQL(target_connection_properties) as psql:
        psql.run(
            {
                "command_type": "update",
                "schema": "public",
                "table": "sys_etl_update",
                "values": f"tm_record='{jobExec.load_partner_data_api_execution}', person_transaction_max_id = 1",
                "where_clause": "1=1",
            }
        )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main)
