import os

from jobControl import jobControl
from pyspark.sql import SparkSession
from utils import arg_utils, dataframe_utils
from utils.spark_request import SparkRequest
from utils.spark_sql import SparkSql

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
jobExec = jobControl.Job(job_name, job_args)


def main():
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    try:
        rdf = SparkSql(spark).sql(jobExec.source_schema)

        url = jobExec.target_schema
        headers = {"Authorization": jobExec.target_table}
        rq = SparkRequest(url, headers)

        rdd = rdf.repartition(20000)
        rdd.foreachPartition(rq.postasync)

    except Exception as e:
        jobExec.logger.warning("Job error" + str(e))


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True)
