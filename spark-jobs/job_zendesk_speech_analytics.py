import json
from datetime import datetime, timedelta

import boto3

# Third Part libs
import pandas as pd
import pysftp
from jobControl import jobControl
from pyspark.sql import SparkSession
from utils import arg_utils

# Read from Apache Airflow:
# - Arguments;
job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]

# Configure and allow airflow to manage this job
print(vars(job_args))
jobExec = jobControl.Job(job_name, job_args)


def load_tables(spark, bucket_name, path_prefix, table_name):
    """Function responsible for reading parquet files.

    This function reads the data catalogs available in
    S3.

    Args:
        spark(:obj:`SparkSession`): Apache spark instance.
        bucket_name (str): Bucket name on AWS s3.
        path_prefix (str): Folder name in AWS s3 bucket.
        table_name (str): Table name.

    Returns:
        int: Returns a DataFrame if successful and
            otherwise None.
    """

    # AWS: Specific bucket path
    path_prefix = path_prefix[1:] if path_prefix[:1] == "/" else path_prefix
    path_prefix = (
        f"{path_prefix}/"
        if path_prefix != "" and path_prefix[-1:] != "/"
        else path_prefix
    )
    table_path = f"s3://{bucket_name}/{path_prefix}{table_name}/"

    data = None

    try:
        data = spark.read.parquet(table_path)
    except Exception as e:
        print(e)
        jobExec.logger.info(f"Table {table_path} not exists.")

    return data


def main():

    # Logging: start
    jobExec.logger.info("start")

    kwargs = {
        "spark": spark,
        "bucket_name": job_args.structured_bucket,
        "path_prefix": job_args.zendesk_bucket_prefix,
    }

    # - Extract some variables from the airflow
    bucket_name = kwargs["bucket_name"]
    path_prefix = kwargs["path_prefix"]

    app_args = json.loads(job_args.zendesk_args)

    file_pre = app_args["file_pre"]
    extracted_day = app_args["extracted_day"]
    sftp_url = app_args["sftp_url"]
    sftp_prt = app_args["sftp_prt"]
    sftp_usr = app_args["sftp_usr"]
    sftp_pwd = app_args["sftp_pwd"]
    sftp_dir = app_args["sftp_dir"]

    print(f"Starting extract from {extracted_day}")

    #  Formats the string that defines the start and end period of the search
    extracted_day_dt = datetime.strptime(extracted_day, "%Y-%m-%d")

    # Log
    print(f"extracted_day_dt = {extracted_day}")

    # Read the comment table and extract some columns
    comments_df = load_tables(spark, bucket_name, path_prefix, "comment")
    ticket_df = load_tables(spark, bucket_name, path_prefix, "ticket")
    ticket_field_df = load_tables(spark, bucket_name, path_prefix, "ticket_field")
    users_df = load_tables(spark, bucket_name, path_prefix, "user")
    user_field_df = load_tables(spark, bucket_name, path_prefix, "user_field")

    # Create temp views
    comments_df.createOrReplaceTempView("comment_temp")
    ticket_df.createOrReplaceTempView("ticket_temp")
    ticket_field_df.createOrReplaceTempView("ticket_field_temp")
    users_df.createOrReplaceTempView("user_temp")
    user_field_df.createOrReplaceTempView("user_field_temp")

    # Join the tables of: comments, tickets and users.
    command_sql = f"""
        SELECT  distinct date_format(comment.comment_date, "yyyy-MM-dd'T'HH:mm:ssX") AS Date, 
            ticket.ticket_id AS Ticket_ID,
            comment.comment AS Comment,
            date_format(ticket.ticket_date, "yyyy-MM-dd'T'HH:mm:ssX") AS Ticket_Date,
            ticket.subject AS Subject,
            ticket.description AS Description,
            ticket.status AS Status,
            ticket.channel AS Channel,
            ticket.tags AS Tags,
            ticket.satisfaction_rating AS `Satisfaction Rating`,
            ticket.custom_channel AS `Custom Channel`,
            comment.comment_author_id AS `Comment Author ID`,
            user.user_id AS `User ID`,
            user.external_id AS `External ID`,
            user.user_name AS `User Name`,
            user.related_gyms_ids AS `Related Gyms Ids`,
            user.plan_name AS `Plan Name`,
            user.current_company_name AS`Current Company Name`
        FROM 
            (SELECT ticket_id, comment_id,
                author_id AS comment_author_id,
                body_masked AS comment,
                created_at AS comment_date
            FROM comment_temp
            WHERE comment_temp.created_part = '{extracted_day}') comment,
            (SELECT ticket_temp.ticket_id, ticket_temp.subject_masked AS subject,
                ticket_temp.description_masked AS description,
                ticket_temp.status,
                ticket_temp.channel,
                ticket_field_temp.value AS custom_channel,
                ticket_temp.tag_group AS tags,
                ticket_temp.satisfaction_rating_score AS satisfaction_rating,
                ticket_temp.created_at AS ticket_date,
                ticket_temp.requester_id
            FROM ticket_temp, ticket_field_temp
            WHERE ticket_field_temp.ticket_id = ticket_temp.ticket_id AND 
                ticket_field_temp.field_id = 360007693174 AND
                ticket_temp.created_part = '{extracted_day}') ticket,
            (SELECT  user_temp.user_id,
                    COALESCE(user_temp.external_id, user_temp.user_id) AS external_id,
                    name AS user_name,
                    CASE 
                        WHEN user_field_temp.field_id = 'managed_gyms_ids' THEN user_field_temp.value 
                        ELSE '' END AS related_gyms_ids,
                    CASE 
                        WHEN user_field_temp.field_id = 'plan_name' THEN user_field_temp.value
                        ELSE '' END AS plan_name,
                    CASE 
                        WHEN user_field_temp.field_id = 'current_company_name' THEN user_field_temp.value 
                        ELSE '' END AS current_company_name
            FROM user_temp, user_field_temp
            WHERE user_temp.user_id = user_field_temp.user_id) user
        WHERE (user.external_id = comment.comment_author_id) AND
            (ticket.ticket_id = comment.ticket_id) AND
            (user.external_id = ticket.requester_id)
        
    """

    # Shows the executed query.
    print(command_sql)

    # Returns the results.
    result_df = spark.sql(command_sql)

    # Sets the filename
    filename = f'{file_pre}_{extracted_day_dt.strftime("%Y%m%d")}.csv'

    # Save the file
    result_df.toPandas().to_csv(filename, index=False, header=True)

    # Send the file to the sftp server
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(
        host=sftp_url,
        port=sftp_prt,
        username=sftp_usr,
        password=sftp_pwd,
        cnopts=cnopts,
    ) as sftp:
        with sftp.cd(sftp_dir):
            sftp.put(filename)

    # Logging: End
    jobExec.logger.info("End")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()

    jobExec.execJob(
        main,
        spark,
        add_hive_path=True,
        delete_excessive_files=True,
        infer_partitions=True,
    )
