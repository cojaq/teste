import datetime
import math
import os

import pandas as pd
from jobControl import jobControl
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from simple_salesforce import Salesforce
from utils import arg_utils, dataframe_utils, utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
num_partitions = 4

jobExec = jobControl.Job(job_name, job_args)
jobExec.target_schema = (
    jobExec.target_schema if jobExec.target_schema else jobExec.database_salesforce_full
)

CUSTOM_TABLES = {
    "CongaContractAgreement": "APXT_Redlining__Contract_Agreement__c",
    "CongaTransaction": "APXT_CongaSign__Transaction__c",
    "CopayPlan": "Copay_Plan__c",
    "GympassEvent": "Gympass_Event__c",
    "GympassLeaderAssociation": "Gympass_Leader_Association__c",
    "StepTowardsSuccess1": "Step_Towards_Success1__c",
    "OpsSetupValidationForm": "Ops_Setup_Validation_Form__c",
    "OpsSetupValidationFormHistory": "Ops_Setup_Validation_Form__History",
    "BankAccount": "Bank_Account__c",
    "Payment": "Payment__c",
    "ProductItem": "Product_Item__c",
    "Eligibility": "Eligibility__c",
    "SalesOrder": "Sales_Order__c",
    "SalesOrderItem": "Sales_Order_Item__c",
    "SalesOrderItemParameter": "Sales_Order_Item_Parameter__c",
    "SalesOrderParameter": "Sales_Order_Parameter__c",
    "Waiver": "Waiver__c",
}
BIG_TABLES = ["task", "step_towards_success1", "conga_contract_agreement"]


def extract_salesforce(dataset_name, target_columns, start_time=None, end_time=None):
    sf = Salesforce(
        username=job_args.database_user,
        password=job_args.database_password,
        security_token=job_args.security_token,
    )
    source_table_name = utils.to_pascal_case(dataset_name)

    if source_table_name in CUSTOM_TABLES:
        source_table_name = CUSTOM_TABLES[source_table_name]

    print(f"\nQuerying Salesforce source table {source_table_name}.\n")

    query = f"""
        SELECT {','.join(target_columns)} 
        FROM {source_table_name}
    """
    if start_time and end_time:
        query = (
            query
            + f""" 
            WHERE createddate > {start_time}
            AND createddate <= {end_time}
        """
        )

    sf_data = sf.query_all(query)

    pd_df = pd.DataFrame(sf_data["records"]).astype(str)
    if pd_df.empty:
        return None
    else:
        return spark.createDataFrame(pd_df).drop("attributes")


def main():
    salesforce_table_columns = dataframe_utils.return_hive_table_columns(
        spark, jobExec.target_schema, jobExec.target_table
    )

    if "createddate" in list(salesforce_table_columns):
        start = datetime.datetime.strptime("2017-01-01", "%Y-%m-%d")
        end = datetime.datetime.today()
        time_window = 90 if jobExec.target_table in BIG_TABLES else 180
        datetimes = [
            (start + datetime.timedelta(days=time_window * x)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for x in range(0, math.ceil((end - start).days / time_window) + 1)
        ]
        sf_dfs = []
        for i in range(len(datetimes) - 1):
            print(
                f"\nExtract Salesforce {jobExec.target_table} table from {datetimes[i]} to {datetimes[i+1]}.\n"
            )
            sf_df = extract_salesforce(
                jobExec.target_table,
                salesforce_table_columns,
                datetimes[i],
                datetimes[i + 1],
            )
            sf_dfs.append(sf_df)

        sf_final_df = dataframe_utils.union_dataframes(sf_dfs)
    else:
        sf_final_df = extract_salesforce(jobExec.target_table, salesforce_table_columns)

    for column in list(salesforce_table_columns):
        sf_final_df = sf_final_df.withColumn(
            column,
            f.when(f.col(column) == "None", None)
            .when(f.col(column) == "nan", None)
            .otherwise(f.col(column)),
        )

    sf_final_df.repartition(num_partitions).write.insertInto(
        f"{jobExec.target_schema}.{jobExec.target_table}", overwrite=True
    )

    if spark._jsparkSession.catalog().tableExists(
        f"{jobExec.target_schema}_history.{jobExec.target_table}"
    ):
        try:
            df_history = sf_final_df.withColumn(
                "reference_date", f.lit(int(jobExec.reference_date))
            )
            df_history = df_history.repartition(num_partitions, "reference_date")
            df_history.write.insertInto(
                f"{jobExec.target_schema}_history.{jobExec.target_table}",
                overwrite=True,
            )
            print(
                f"Success - inserted to History on {jobExec.target_schema}_history.{jobExec.target_table}"
            )
        except Exception as err:
            print(
                f"Error - failed to insert History on {jobExec.target_schema}_history.{jobExec.target_table}"
            )
            print(f"Error: - {err}")
            pass

    jobExec.totalLines = sf_final_df.count()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True, delete_excessive_files=True)
