import os
from datetime import datetime, timedelta

from jobControl import jobControl
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from utils import arg_utils

job_args = arg_utils.get_job_args()
job_name = os.path.basename(__file__).split(".")[0]
jobExec = jobControl.Job(job_name, job_args)

notificationEventTriggeredTable = "notification_event_triggered"
sendgridNotificationEventTriggeredTable = "sendgrid_notification_event_triggered"

leadTimeTable = "lead_time"
leadTimeOverviewTable = "lead_time_overview"

defaultNumberOfDaysBeforeToday = 2

notificationEventTriggeredStatusCombos = {
    "enqueued": ["sender"],
    "sender": ["error"],
}

sendgridNotificationEventTriggeredStatusCombos = {
    "processed": ["dropped", "deferred", "bounce", "delivered"],
    "delivered": ["open"],
    "open": ["click"],
}


# apply the "start_date" and "stop_date" parameters to the query
def filterByDate(dataFrame):
    if jobExec.start_date == "null":
        jobExec.start_date = (
            datetime.today() - timedelta(days=defaultNumberOfDaysBeforeToday)
        ).strftime("%Y%m%d")

    if jobExec.stop_date == "null":
        jobExec.stop_date = (datetime.today() + timedelta(days=1)).strftime("%Y%m%d")

    jobExec.logger.info(
        f"filtering by date: {jobExec.start_date} and {jobExec.stop_date}"
    )

    return dataFrame.filter((f.col("dt") >= jobExec.start_date)).filter(
        (f.col("dt") < jobExec.stop_date)
    )


def addFieldsToAndChannel(dataFrame):
    return dataFrame.withColumn(
        "channel",
        f.when(f.col("channel").isNull(), f.lit("email")).otherwise(
            dataFrame["channel"]
        ),
    ).withColumn(
        "to",
        f.when(f.col("to").isNull(), dataFrame["email"]).otherwise(dataFrame["to"]),
    )


# creates the notification_event_triggered dataframe
def createNotificationEventTriggeredDF():
    dataFrame = filterByDate(
        spark.table(f"{jobExec.source_schema}.{notificationEventTriggeredTable}")
    ).filter((f.col("event_raw.audit_id").isNotNull()))

    return addFieldsToAndChannel(dataFrame).select(
        col("_event_time").alias("net_event_epoch"),
        col("notification_type").alias("net_notification_type"),
        col("notification_status").alias("net_notification_status"),
        col("event_raw.audit_id").alias("net_audit_id"),
        col("email").alias("net_email"),
        col("to").alias("net_to"),
        col("channel").alias("net_channel"),
    )


# creates the sendgrid_notification_event_triggered dataframe
def createSendgridNotificationEventTriggeredDF():
    dataFrame = filterByDate(
        spark.table(
            f"{jobExec.source_schema}.{sendgridNotificationEventTriggeredTable}"
        )
    ).filter(f.col("audit_id").isNotNull())

    return addFieldsToAndChannel(dataFrame).select(
        col("_event_time").alias("snet_event_epoch"),
        col("category")[0].alias("snet_notification_type"),
        col("event").alias("snet_notification_status"),
        col("audit_id").alias("snet_audit_id"),
        col("email").alias("snet_email"),
        col("to").alias("snet_to"),
        col("channel").alias("snet_channel"),
    )


# replaces the prefixes from all columns from the dataframe
def changePrefixesFromColumns(dataFrame, prefix):
    return dataFrame.toDF(
        f"{prefix}_event_epoch",
        f"{prefix}_notification_type",
        f"{prefix}_notification_status",
        f"{prefix}_audit_id",
        f"{prefix}_email",
        f"{prefix}_to",
        f"{prefix}_channel",
    )


# calculates the total times between phases
def calculateTotalTimesBetweenPhases(dataFrame):
    return (
        dataFrame.withColumn(
            "total_time",
            dataFrame.event_epoch_end - dataFrame.event_epoch_start,
        )
        .select(
            col("audit_id").alias("event_id"),
            col("notification_type"),
            col("notification_status_start").alias("starting_notification_status"),
            col("notification_status_end").alias("ending_notification_status"),
            col("event_epoch_start").alias("starting_time"),
            col("event_epoch_end").alias("ending_time"),
            col("total_time"),
            col("month"),
            col("email"),
            col("to"),
            col("channel"),
            col("dt"),
        )
        .filter(col("total_time") >= 0)
        .sort(col("starting_time").asc())
    )


# join both dataframes removing duplicated final status
def joinNotifications(
    dfNotificationEventTriggered, dfSendgridNotificationEventTriggered
):
    return (
        dfNotificationEventTriggered.join(
            dfSendgridNotificationEventTriggered,
            dfNotificationEventTriggered.net_audit_id
            == dfSendgridNotificationEventTriggered.snet_audit_id,
            "inner",
        )
        .select(
            col("net_audit_id").alias("audit_id"),
            col("net_notification_type").alias("notification_type"),
            col("net_event_epoch").alias("event_epoch_start"),
            col("snet_event_epoch").alias("event_epoch_end"),
            col("net_notification_status").alias("notification_status_start"),
            col("snet_notification_status").alias("notification_status_end"),
            col("net_email").alias("email"),
            col("net_to").alias("to"),
            col("net_channel").alias("channel"),
        )
        .withColumn(
            "row_num",
            f.row_number().over(
                Window.partitionBy(
                    [
                        col("audit_id"),
                        col("notification_status_start"),
                        col("notification_status_end"),
                    ]
                ).orderBy(col("event_epoch_end").asc())
            ),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# apply the status filter to the specified dataframe
def filterByStatus(dataFrame, columnPrefix, statuses):
    return dataFrame.filter(col(f"{columnPrefix}_notification_status").isin(statuses))


# calculates the lead total time
def calculateLeadTotalTime(dataFrame):
    dfTimestamps = dataFrame.withColumn(
        "email_domain", f.split(col("email"), "@")[1]
    ).select(
        col("event_id").alias("event_id_ts"),
        col("starting_time"),
        col("ending_time"),
        col("email"),
        col("email_domain"),
        col("channel"),
        col("to"),
    )

    dfLeadTotalTime = (
        dataFrame.groupBy(
            col("event_id"),
            col("month"),
            col("dt"),
            col("notification_type"),
            col("starting_notification_status"),
            col("ending_notification_status"),
        )
        .agg(f.sum("total_time").alias("total_time"))
        .select(
            col("event_id"),
            col("notification_type"),
            col("starting_notification_status"),
            col("ending_notification_status"),
            col("total_time"),
            col("month"),
            col("dt"),
        )
    )

    return dfLeadTotalTime.join(
        dfTimestamps,
        dfLeadTotalTime.event_id == dfTimestamps.event_id_ts,
        "inner",
    ).select(
        col("event_id"),
        col("notification_type"),
        col("starting_notification_status"),
        col("ending_notification_status"),
        col("starting_time"),
        col("ending_time"),
        col("total_time"),
        col("month"),
        col("email"),
        col("email_domain"),
        col("channel"),
        col("to"),
        col("dt"),
    )


# calculates the average from all phases
def calculateLeadAggregations(dataFrame):
    return dataFrame.groupBy(
        col("month"),
        col("dt"),
        col("notification_type"),
        col("starting_notification_status"),
        col("ending_notification_status"),
        col("channel"),
    ).agg(
        f.sum("total_time").alias("total_time_sum"),
        f.avg("total_time").alias("total_time_average"),
        f.stddev("total_time").alias("total_time_std_dev"),
    )


# unites all the join combinations
def uniteJoins(dfNotificationEventTriggered, dfSendgridNotificationEventTriggered):
    mainUnion = None

    for (
        startStatus,
        endStatuses,
    ) in notificationEventTriggeredStatusCombos.items():
        join = joinNotifications(
            filterByStatus(dfNotificationEventTriggered, "net", [startStatus]),
            changePrefixesFromColumns(  # special case to handle both starting statuses
                filterByStatus(dfNotificationEventTriggered, "net", endStatuses),
                "snet",
            ),
        )

        if mainUnion == None:
            mainUnion = join
        else:
            mainUnion = mainUnion.union(join)
            join.unpersist(True)

    for (
        startStatus,
        endStatuses,
    ) in sendgridNotificationEventTriggeredStatusCombos.items():
        join = joinNotifications(
            changePrefixesFromColumns(  # special case to handle both starting statuses
                filterByStatus(
                    dfSendgridNotificationEventTriggered,
                    "snet",
                    [startStatus],
                ),
                "net",
            ),
            filterByStatus(dfSendgridNotificationEventTriggered, "snet", endStatuses),
        )

        if mainUnion == None:
            mainUnion = join
        else:
            mainUnion = mainUnion.union(join)
            join.unpersist(True)

    mainJoin = joinNotifications(
        dfNotificationEventTriggered, dfSendgridNotificationEventTriggered
    )

    lastUnion = mainUnion.union(mainJoin)

    result = addDatesColumns(lastUnion.orderBy(col("audit_id").asc()))
    lastUnion.unpersist(True)

    return result


# adds the month column based in the event epoch start column
def addDatesColumns(dataFrame):
    return dataFrame.withColumn(
        "month",
        f.date_format(
            (col("event_epoch_start") / 1000).cast(TimestampType()), "YYYYMM"
        ),
    ).withColumn(
        "dt",
        f.date_format(
            (col("event_epoch_start") / 1000).cast(TimestampType()),
            "YYYYMMdd",
        ),
    )


# calculate the quantiles: 0.25, 0.5, 0.75, 0.99
def calculateQuantiles(dataFrame):
    windowPartition = Window.partitionBy(
        [
            col("notification_type"),
            col("starting_notification_status"),
            col("ending_notification_status"),
            col("month"),
            col("channel"),
            col("dt"),
        ]
    )

    quantiles = f.expr(
        "percentile_approx(total_time, array(0.1, 0.25, 0.5, 0.75, 0.99))"
    )

    return (
        dataFrame.withColumn("quantiles_calc", quantiles.over(windowPartition))
        .groupBy(
            col("notification_type"),
            col("starting_notification_status"),
            col("ending_notification_status"),
            col("month"),
            col("channel"),
            col("dt"),
            col("quantiles_calc").alias("quantiles"),
        )
        .count()
    )


# apply repartition function over "dt" column
def applyRepartition(dataFrame):
    return dataFrame.repartition(int(jobExec.num_repartitions), "dt")


# join aggregations and quantiles dataframes to produce the final overview dataframe
def joinAggregationsAndQuantiles(dfAggregations, dfQuantiles):
    dfQuantilesAlias = dfQuantiles.select(
        col("notification_type").alias("notification_type_qtl"),
        col("starting_notification_status").alias("starting_notification_status_qtl"),
        col("ending_notification_status").alias("ending_notification_status_qtl"),
        col("channel").alias("channel_qtl"),
        col("month").alias("month_qtl"),
        col("dt").alias("dt_qtl"),
        col("quantiles"),
        col("count"),
    )

    dfAggregationsAlias = dfAggregations.select(
        col("notification_type").alias("notification_type_agg"),
        col("starting_notification_status").alias("starting_notification_status_agg"),
        col("ending_notification_status").alias("ending_notification_status_agg"),
        col("channel").alias("channel_agg"),
        col("month").alias("month_agg"),
        col("dt").alias("dt_agg"),
        col("total_time_sum"),
        col("total_time_average"),
        col("total_time_std_dev"),
    )

    return (
        dfAggregationsAlias.join(
            dfQuantilesAlias,
            (
                dfAggregationsAlias.notification_type_agg
                == dfQuantilesAlias.notification_type_qtl
            )
            & (
                dfAggregationsAlias.starting_notification_status_agg
                == dfQuantilesAlias.starting_notification_status_qtl
            )
            & (
                dfAggregationsAlias.ending_notification_status_agg
                == dfQuantilesAlias.ending_notification_status_qtl
            )
            & (dfAggregationsAlias.channel_agg == dfQuantilesAlias.channel_qtl)
            & (dfAggregationsAlias.month_agg == dfQuantilesAlias.month_qtl)
            & (dfAggregationsAlias.dt_agg == dfQuantilesAlias.dt_qtl),
            "inner",
        )
        .select(
            col("notification_type_agg").alias("notification_type"),
            col("starting_notification_status_agg").alias(
                "starting_notification_status"
            ),
            col("ending_notification_status_agg").alias("ending_notification_status"),
            col("channel_agg").alias("channel"),
            col("month_agg").alias("month"),
            col("dt_agg").alias("dt"),
            col("quantiles"),
            col("count"),
            col("total_time_sum"),
            col("total_time_average"),
            col("total_time_std_dev"),
        )
        .withColumn(
            "aggregations",
            f.create_map(
                f.lit("quantiles"),
                f.concat_ws(",", col("quantiles")).cast("string"),
                f.lit("count"),
                col("count").cast("string"),
                f.lit("total_time_sum"),
                col("total_time_sum").cast("string"),
                f.lit("total_time_average"),
                col("total_time_average").cast("string"),
                f.lit("total_time_std_dev"),
                col("total_time_std_dev").cast("string"),
            ),
        )
        .withColumn("channel", f.lit("email"))
        .drop(
            "quantiles",
            "count",
            "total_time_sum",
            "total_time_average",
            "total_time_std_dev",
        )
        .select(
            col("notification_type"),
            col("starting_notification_status"),
            col("ending_notification_status"),
            col("month"),
            col("aggregations"),
            col("channel"),
            col("dt"),
        )
    )


def printDataFrame(df):
    df.show(df.count(), False)


def main():
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    jobExec.logger.info(f"using number of repartitions as: {jobExec.num_repartitions}")

    dfNotificationEventTriggered = createNotificationEventTriggeredDF()
    dfSendgridNotificationEventTriggered = createSendgridNotificationEventTriggeredDF()
    union = uniteJoins(
        dfNotificationEventTriggered, dfSendgridNotificationEventTriggered
    )

    dfNotificationEventTriggered.unpersist(True)
    dfSendgridNotificationEventTriggered.unpersist(True)
    lines = union.count()

    jobExec.totalLines = lines

    if lines == 0:
        jobExec.logger.warning("Skipping extract job because query return zero results")
        return

    union = applyRepartition(union)

    resultsWithPhasesTimes = applyRepartition(calculateTotalTimesBetweenPhases(union))

    union.unpersist(True)

    dfTotalTime = applyRepartition(calculateLeadTotalTime(resultsWithPhasesTimes))

    dfTotalTime.write.insertInto(
        f"{jobExec.target_schema}.{leadTimeTable}", overwrite=True
    )
    dfTotalTime.unpersist(True)

    jobExec.logger.info(f"{jobExec.target_schema}.{leadTimeTable} saved!")

    dfAggregations = applyRepartition(calculateLeadAggregations(resultsWithPhasesTimes))

    dfQuantiles = applyRepartition(calculateQuantiles(dfTotalTime))

    dfOverview = applyRepartition(
        joinAggregationsAndQuantiles(dfAggregations, dfQuantiles)
    )

    dfAggregations.unpersist(True)
    dfQuantiles.unpersist(True)

    dfOverview.write.insertInto(
        f"{jobExec.target_schema}.{leadTimeOverviewTable}", overwrite=True
    )

    jobExec.logger.info(f"{jobExec.target_schema}.{leadTimeOverviewTable} saved!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    jobExec.execJob(main, spark, add_hive_path=True)
