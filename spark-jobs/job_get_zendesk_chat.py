import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

# Third Part libs
import requests
import zendesk_model
from jobControl import jobControl
from pyspark.sql import SparkSession
from utils import arg_utils

###############################################################################


def _info(msg):
    jobExec.logger.info(msg)
    # print(f'{datetime.now()} {msg}')


def _error(msg):
    jobExec.logger.error(msg)
    # print(f'{datetime.now()} ERROR')
    # print(f'{datetime.now()} {msg}')


###############################################################################


def _str(value):
    return str(value) if value != None else None


def _int(value):
    return int(value) if value != None else None


def _float(value):
    return float(value) if value != None else None


def _ts(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ") if value != None else None


def _bool(value):
    if value not in [None, 0, 1]:
        raise Exception(
            f"Value {value} not in Boolean function. 0 (False) or 1 (True)."
        )
    return True if value == 1 else False


def _rebase(page, base, new):
    return base + [
        {
            **new,
            **{
                "_extract_page": page,
                "_extract_timestamp": datetime.utcnow().timestamp(),
            },
        }
    ]


###############################################################################


def _extract_incremental_chats(data, page, mode, kwargs):

    # base
    base = []
    base_tag = []
    base_agent_id = []
    base_agent_name = []
    base_history = []

    for row in data["chats"]:
        base = _rebase(
            page,
            base,
            {
                "chat_id": _str(row["id"]),
                "ticket_id": _int(row["zendesk_ticket_id"]),
                "department_id": _int(
                    row["department_id"] if "department_id" in row else None
                ),
                "department_name": _str(
                    row["department_name"] if "department_name" in row else None
                ),
                "type": _str(row["type"]),
                "started_by": _str(row["started_by"] if "started_by" in row else None),
                "rating": _str(row["rating"] if "rating" in row else None),
                "comment": _str(row["comment"] if "comment" in row else None),
                "served_chat": _float(None),
                "transferred_chat": _float(None),
                "completed_chat": _float(None),
                "good_satisfaction_chat": _float(None),
                "bad_satisfaction_chat": _float(None),
                "missed": _bool(row["missed"] if "missed" in row else None),
                "visitor_id": _str(row["visitor"]["id"]),
                "visitor_name": _str(row["visitor"]["name"]),
                "visitor_email": _str(row["visitor"]["email"]),
                "visitor_phone": _str(row["visitor"]["phone"]),
                "visitor_notes": _str(row["visitor"]["notes"]),
                "session_id": _str(row["session"]["id"]),
                "session_started_at": _ts(row["session"]["start_date"]),
                "session_ended_at": _ts(row["session"]["end_date"]),
                "session_country_name": _str(row["session"]["country_name"]),
                "session_country_code": _str(row["session"]["country_code"]),
                "session_region": _str(row["session"]["region"]),
                "session_city": _str(row["session"]["city"]),
                "session_ip_address": _str(row["session"]["ip"]),
                "session_platform": _str(row["session"]["platform"]),
                "session_browser": _str(row["session"]["browser"]),
                "session_user_agent": _str(row["session"]["user_agent"]),
                "count_visitor": _int(
                    row["count"]["visitor"]
                    if "count" in row and "visitor" in row["count"]
                    else None
                ),
                "count_agent": _int(
                    row["count"]["agent"]
                    if "count" in row and "agent" in row["count"]
                    else None
                ),
                "count_total": _int(
                    row["count"]["total"]
                    if "count" in row and "total" in row["count"]
                    else None
                ),
                "duration": _float(row["duration"] if "duration" in row else None),
                "duration_wait_time": _float(None),
                "duration_first_reply": _float(
                    row["response_time"]["first"]
                    if "response_time" in row and "first" in row["response_time"]
                    else None
                ),
                "duration_avg_reply": _float(
                    row["response_time"]["avg"]
                    if "response_time" in row and "avg" in row["response_time"]
                    else None
                ),
                "duration_max_reply": _float(
                    row["response_time"]["max"]
                    if "response_time" in row and "max" in row["response_time"]
                    else None
                ),
                "duration_range_sla": _float(None),
                "created_at": _ts(row["timestamp"]),
                "updated_at": _ts(row["update_timestamp"]),
                "created_part": _str(row["timestamp"][:10]),
            },
        )

        if "tags" in row:
            for row_tag in row["tags"]:
                base_tag = _rebase(
                    page,
                    base_tag,
                    {
                        "chat_id": _str(row["id"]),
                        "tag": _str(row_tag),
                        "created_part": _str(row["timestamp"][:10]),
                    },
                )

        if "agent_ids" in row:
            for row_agent_id in row["agent_ids"]:
                base_agent_id = _rebase(
                    page,
                    base_agent_id,
                    {
                        "chat_id": _str(row["id"]),
                        "agent_id": _int(row_agent_id),
                        "created_part": _str(row["timestamp"][:10]),
                    },
                )

        if "agent_names" in row:
            for row_agent_name in row["agent_names"]:
                base_agent_name = _rebase(
                    page,
                    base_agent_name,
                    {
                        "chat_id": _str(row["id"]),
                        "agent_name": _str(row_agent_name),
                        "created_part": _str(row["timestamp"][:10]),
                    },
                )

        if "history" in row:
            for row_history in row["history"]:
                base_history = _rebase(
                    page,
                    base_history,
                    {
                        "chat_id": _str(row["id"]),
                        "index": _int(
                            row_history["index"] if "index" in row_history else None
                        ),
                        "name": _str(
                            row_history["name"] if "name" in row_history else None
                        ),
                        "type": _str(
                            row_history["type"] if "type" in row_history else None
                        ),
                        "timestamp": _ts(row_history["timestamp"]),
                        "agent_id": _int(
                            row_history["agent_id"]
                            if "agent_id" in row_history
                            else None
                        ),
                        "department_id": _int(
                            row_history["department_id"]
                            if "department_id" in row_history
                            else None
                        ),
                        "department_name": _str(
                            row_history["department_name"]
                            if "department_name" in row_history
                            else None
                        ),
                        "channel": _str(
                            row_history["channel"] if "channel" in row_history else None
                        ),
                        "sender_type": _str(
                            row_history["sender_type"]
                            if "sender_type" in row_history
                            else None
                        ),
                        "msg_id": _int(
                            row_history["msg_id"] if "msg_id" in row_history else None
                        ),
                        "msg": _str(
                            row_history["msg"] if "msg" in row_history else None
                        ),
                        "options": _str(
                            row_history["options"] if "options" in row_history else None
                        ),
                        "created_part": _str(row["timestamp"][:10]),
                    },
                )

    _persist("chat", base, mode, kwargs)
    _persist("chat_tag", base_tag, mode, kwargs)
    _persist("chat_agent_id", base_agent_id, mode, kwargs)
    _persist("chat_agent_name", base_agent_name, mode, kwargs)
    _persist("chat_history", base_history, mode, kwargs)


###############################################################################


def _get_switch_base(start_time):

    return {
        "incremental_chats": {
            "url": f"/api/v2/incremental/chats?start_time={start_time}&fields=chats(*)",
            "func": _extract_incremental_chats,
            "checkpoint": True,
        }
    }


###############################################################################


def _persist(base, data, mode, kwargs):

    size = len(data)

    schema = zendesk_model.get_extract_schema(base)

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]
    extract_path = f"s3://{extract_bucket}/{extract_prefix}{base}/"

    df = spark.createDataFrame(data, schema)

    df.write.mode(mode).parquet(extract_path)
    _info(f"Parquet {extract_path} {mode} - Wrote {size} successfully.")


###############################################################################


def extract(base, kwargs):

    switch_base = _get_switch_base(kwargs["start_timestamp"])

    try:
        _info(f"Selecting {base} base.")
        select_base = switch_base[base]
    except:
        possible_values = ", ".join(list(switch_base.keys()))
        err_msg = f"Error selecting base {base}. Possible values: {possible_values}."
        _error(err_msg)
        raise Exception(err_msg)

    errors_max = kwargs["errors_max"]
    pages_max = kwargs["pages_max"]
    bucket = kwargs["extract_bucket"]
    prefix = kwargs["extract_prefix"]

    zendesk_headers = {
        "Content-Type": "application/json",
        "Authorization": kwargs["api_auth"],
    }

    # First URL
    url = "https://" + kwargs["api_url"] + select_base["url"]
    if select_base["checkpoint"] and kwargs["load_type"] in [
        "full",
        "continue",
        "finale",
    ]:
        try:
            url = (
                spark.read.parquet(f"s3://{bucket}/{prefix}/control/{base}/")
                .select(base)
                .take(1)[0][0]
            )
        except:
            _error(
                f"Error reading s3://{bucket}/{prefix}/control/{base}/ parquet. It will continue with {url}."
            )

    mode = "overwrite"
    if select_base["checkpoint"] and kwargs["load_type"] in [
        "continue",
        "finale",
    ]:
        mode = "append"

    # Starts capturing API data
    pages_count = 0
    while url != None:

        # Checkout URL
        if (
            select_base["checkpoint"]
            and kwargs["load_type"] in ["full", "historical", "continue", "finale"]
            and pages_count != 0
        ):
            spark.createDataFrame([{base: url}]).write.mode("overwrite").parquet(
                f"s3://{bucket}/{prefix}/control/{base}/"
            )

        _info(f"Page {pages_count+1}: {url}")

        errors_count = 0
        data = None
        while data == None:

            try:
                resp = requests.get(url, headers=zendesk_headers)
            except Exception as e:
                _error(f"Request Error. Message {e}.")

                errors_count += 1
                if errors_count == errors_max:
                    raise Exception(f"Abort after {errors_max} requests error.")

                sleep_seconds = kwargs["sleep_default"]
                _info(f"Retry in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                continue

            if resp.status_code == 429:
                errors_count += 1
                if errors_count == errors_max:
                    raise Exception(f"Abort after {errors_max} requests error.")

                sleep_seconds = int(resp.headers.get("Retry-After"))
                _info(f"Retry in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                continue

            if resp.status_code != 200:
                _error(f"Request Error. Status Code {resp.status_code}.")

                errors_count += 1
                if errors_count == errors_max:
                    raise Exception(
                        f"Error: {resp.status_code}. Abort after {errors_max} requests error."
                    )

                sleep_seconds = kwargs["sleep_default"]
                _info(f"Retry in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                continue

            data = resp.json()

        if data and data.get("count", 0) > 0:
            func = select_base["func"]
            func(data, pages_count, mode, kwargs)
            pages_count += 1

        url = None
        mode = "append"
        if (
            (data.get("count", 0) > 0)
            and "next_page" in data
            and data["next_page"] != None
            and pages_count < pages_max
        ):
            url = data["next_page"]
        elif pages_count > pages_max:
            _info(f"Terminate by hitting pages_max: {pages_max}.")
        else:
            _info("There are no data to processing.")


###############################################################################


def main():

    _info("Start")

    app_args = json.loads(job_args.zendesk_args)

    extract_strategy = app_args["extract_strategy"]

    # delta
    if extract_strategy == "full":
        load_type = "full"
        start_date = 0
    elif extract_strategy.startswith("historical"):
        load_type = "historical"
        start_date = _ts(extract_strategy.split(" ")[1])
    elif extract_strategy == "continue":
        load_type = "continue"
        start_date = 0
    elif extract_strategy == "finale":
        load_type = "finale"
        start_date = 0
    elif "days" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            days=int(extract_strategy.replace("days", ""))
        )
    elif "day" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            days=int(extract_strategy.replace("day", ""))
        )
    elif "hours" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            hours=int(extract_strategy.replace("hours", ""))
        )
    elif "hour" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            hours=int(extract_strategy.replace("hour", ""))
        )
    elif "minutes" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            minutes=int(extract_strategy.replace("minutes", ""))
        )
    elif "minute" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            minutes=int(extract_strategy.replace("minute", ""))
        )
    else:
        raise Exception(
            "Invalid Extract Strategy. Possible values: Full, Continue, Finale, X days/hours/minutes, Historical 2020-01-01T00:00:00Z."
        )

    if start_date != 0:
        start_timestamp = str(
            int(start_date.replace(tzinfo=timezone.utc).astimezone(tz=None).timestamp())
        )
    else:
        start_timestamp = "0"

    bucket = app_args["extract_bucket"]
    prefix = (
        app_args["extract_prefix"]
        if app_args["extract_prefix"][-1:] == "/"
        else app_args["extract_prefix"] + "/"
    )

    kwargs = {
        "spark": spark,
        "extract_bucket": bucket,
        "extract_prefix": prefix,
        "api_url": app_args["api_url"],
        "api_auth": app_args["api_auth"],
        "pages_max": float("inf")
        if app_args["pages_max"] == "none"
        else int(app_args["pages_max"]),
        "errors_max": int(app_args["errors_max"]),
        "sleep_default": int(app_args["sleep_default"]),
        "load_type": load_type,
        "start_date": start_date,
        "start_timestamp": start_timestamp,
    }

    if app_args["base"] == "check_extract_strategy":

        # Se full, zerar urls
        if load_type == "full":
            switch_base = _get_switch_base("0")
            base_list = [
                (base, switch_base[base]["url"])
                for base in switch_base
                if switch_base[base]["checkpoint"]
            ]

            for base, endpoint in base_list:

                spark.createDataFrame(
                    [{base: "https://" + app_args["api_url"] + endpoint}]
                ).write.mode("overwrite").parquet(
                    f"s3://{bucket}/{prefix}/control/{base}/"
                )

    else:

        extract(app_args["base"], kwargs)

    _info("End")


###############################################################################

if __name__ == "__main__":

    job_args = arg_utils.get_job_args()
    job_name = os.path.basename(__file__).split(".")[0]

    jobExec = jobControl.Job(job_name, job_args)

    spark = SparkSession.builder.appName(job_name).getOrCreate()

    jobExec.execJob(
        main,
        spark,
        add_hive_path=False,
        delete_excessive_files=False,
        infer_partitions=False,
    )
