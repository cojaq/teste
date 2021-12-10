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
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)
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


def _str_mask(value):
    return re.sub(
        r"([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+|(Phone:\s*)([^0-9]*[0-9]){6}|[0-9]{3}[\.-]?[0-9]{3}[\.-]?[0-9]{3}[\.-]?[0-9]{2})",
        r"\2{confidential}",
        _str(value),
    )


def _int(value):
    return int(value) if value != None else None


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


def _extract_organization_fields(data, page, kwargs):

    base = []

    for row in data["organization_fields"]:

        # base
        base = _rebase(
            page,
            base,
            {
                "field_id": _str(row["key"]),
                "title": _str(row["title"]),
                "type": _str(row["type"]),
                "description": _str(row["description"]),
                "raw_title": _str(row["raw_title"]),
                "raw_description": _str(row["raw_description"]),
            },
        )

    return [["cf_org", base]]


def _extract_incremental_organizations(data, page, kwargs):

    base = []
    base_field = []
    base_tag = []

    for row in data["organizations"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        # base
        base = _rebase(
            page,
            base,
            {
                "org_id": _int(row["id"]),
                "name": _str(row["name"]),
                "external_id": _str(row["external_id"]),
                "details": _str(row["details"]),
                "notes": _str(row["notes"]),
                "group_id": _int(row["group_id"]),
                "domain_names": ",".join(row["domain_names"]),
                "created_at": _ts(row["created_at"]),
                "updated_at": _ts(row["updated_at"]),
                "deleted_at": _ts(row["deleted_at"]),
                "shared_tickets": _bool(row["shared_tickets"]),
                "shared_comments": _bool(row["shared_comments"]),
                "created_part": _str(row["created_at"][:10]),
            },
        )

        # field
        for row_field in row["organization_fields"]:
            if row["organization_fields"][row_field] is not None:
                base_field = _rebase(
                    page,
                    base_field,
                    {
                        "org_id": _int(row["id"]),
                        "field_id": _str(row_field),
                        "value": _str(row["organization_fields"][row_field]),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

        # tag
        for row_tag in row["tags"]:
            base_tag = _rebase(
                page,
                base_tag,
                {
                    "org_id": _int(row["id"]),
                    "tag": _str(row_tag),
                    "created_part": _str(row["created_at"][:10]),
                },
            )

    return [
        ["org", base],
        ["org_field", base_field],
        ["org_tag", base_tag],
    ]


def _extract_user_fields(data, page, kwargs):

    base = []

    for row in data["user_fields"]:
        base = _rebase(
            page,
            base,
            {
                "field_id": _str(row["key"]),
                "title": _str(row["title"]),
                "type": _str(row["type"]),
                "description": _str(row["description"]),
                "raw_title": _str(row["raw_title"]),
                "raw_description": _str(row["raw_description"]),
            },
        )

    return [["cf_user", base]]


def _extract_incremental_users(data, page, kwargs):

    # base
    base = []
    base_tag = []
    base_field = []
    base_identity = []

    for row in data["users"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        # Create custom field
        chat_only = False
        if row["role_type"] is not None and row["role_type"] == 2:
            chat_only = True

        # base
        base = _rebase(
            page,
            base,
            {
                "user_id": _int(row["id"]),
                "external_id": _str(row["external_id"]),
                "role": _str(row["role"]),
                "role_type": _str(row["role_type"]),
                "custom_role_id": _str(row["custom_role_id"]),
                "email": _str(row["email"]),
                "created_at": _ts(row["created_at"]),
                "updated_at": _ts(row["updated_at"]),
                "org_id": _int(row["organization_id"]),
                "default_group_id": _int(row["default_group_id"]),
                "active": _bool(row["active"]),
                "alias": _str(row["alias"]),
                "details": _str(row["details"]),
                "last_login_at": _ts(row["last_login_at"]),
                "locale": _str(row["locale"]),
                "locale_id": _int(row["locale_id"]),
                "name": _str(row["name"]),
                "notes": _str(row["notes"]),
                "only_private_comments": _bool(row["only_private_comments"]),
                "photo_url": _str(
                    row["photo"]["content_url"]
                    if "photo" in row
                    and row["photo"] != None
                    and "content_url" in row["photo"]
                    else None
                ),
                "time_zone": _str(row["time_zone"]),
                "iana_time_zone": _str(row["iana_time_zone"]),
                "url": _str(row["url"]),
                "phone": _str(row["phone"]),
                "shared_phone_number": _bool(row["shared_phone_number"]),
                "verified": _bool(row["verified"]),
                "shared": _bool(row["shared"]),
                "shared_agent": _bool(row["shared_agent"]),
                "restricted_agent": _bool(row["restricted_agent"]),
                "two_factor_auth_enabled": _bool(row["two_factor_auth_enabled"]),
                "signature": _str(row["signature"]),
                "moderator": _bool(row["moderator"]),
                "ticket_restriction": _str(row["ticket_restriction"]),
                "suspended": _bool(row["suspended"]),
                "chat_only": chat_only,
                "report_csv": _bool(row["report_csv"]),
                "created_part": _str(row["created_at"][:10]),
            },
        )

        # tag
        if "tags" in row:
            for row_tag in row["tags"]:
                base_tag = _rebase(
                    page,
                    base_tag,
                    {
                        "user_id": _int(row["id"]),
                        "tag": _str(row_tag),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

        # field
        if "user_fields" in row:
            for row_field in row["user_fields"]:
                if row["user_fields"][row_field] != None:
                    base_field = _rebase(
                        page,
                        base_field,
                        {
                            "user_id": _int(row["id"]),
                            "field_id": _str(row_field),
                            "value": _str(row["user_fields"][row_field]),
                            "created_part": _str(row["created_at"][:10]),
                        },
                    )

    if "identities" in data:
        for row in data["identities"]:

            if kwargs["load_type"] == "historical" and not (
                kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
            ):
                continue

            # identity
            if "id" in row:
                base_identity = _rebase(
                    page,
                    base_identity,
                    {
                        "user_id": _int(row["user_id"]),
                        "identity_id": _int(row["id"]),
                        "url": _str(row["url"]),
                        "type": _str(row["type"]),
                        "value": _str(row["value"]),
                        "verified": _bool(row["verified"]),
                        "primary": _bool(row["primary"]),
                        "created_at": _ts(row["created_at"]),
                        "updated_at": _ts(row["updated_at"]),
                        "undeliverable_count": _int(
                            row.get("undeliverable_count", None)
                        ),
                        "deliverable_state": _str(row.get("deliverable_state", None)),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

    return [
        ["user", base],
        ["user_tag", base_tag],
        ["user_field", base_field],
        ["user_identity", base_identity],
    ]


def _extract_groups(data, page, kwargs):

    base = []

    for row in data["groups"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        base = _rebase(
            page,
            base,
            {
                "group_id": _int(row["id"]),
                "url": _str(row["url"]),
                "name": _str(row["name"]),
                "description": _str(row["description"]),
                "default": _bool(row["default"]),
                "deleted": _bool(row["deleted"]),
                "created_at": _ts(row["created_at"]),
                "updated_at": _ts(row["updated_at"]),
                "created_part": _str(row["created_at"][:10]),
            },
        )

    return [["group", base]]


def _extract_group_memberships(data, page, kwargs):

    base = []

    for row in data["group_memberships"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        base = _rebase(
            page,
            base,
            {
                "membership_id": _int(row["id"]),
                "user_id": _int(row["user_id"]),
                "group_id": _int(row["group_id"]),
                "url": _str(row["url"]),
                "default": _bool(row["default"]),
                "created_at": _ts(row["created_at"]),
                "updated_at": _ts(row["updated_at"]),
                "created_part": _str(row["created_at"][:10]),
            },
        )

    return [["group_membership", base]]


def _extract_ticket_forms(data, page, kwargs):

    base = []

    for row in data["ticket_forms"]:

        base = _rebase(
            page,
            base,
            {"form_id": _int(row["id"]), "form_name": _str(row["name"])},
        )

    return [["form", base]]


def _extract_ticket_fields(data, page, kwargs):

    base = []

    for row in data["ticket_fields"]:

        base = _rebase(
            page,
            base,
            {
                "field_id": _str(row["id"]),
                "title": _str(row["title"]),
                "type": _str(row["type"]),
                "description": _str(row["description"]),
                "raw_title": _str(row["raw_title"]),
                "raw_description": _str(row["raw_description"]),
                "custom_field_options": _str(
                    row["custom_field_options"]
                    if "custom_field_options" in row
                    else None
                ),
            },
        )

    return [["cf_ticket", base]]


def _extract_incremental_tickets(data, page, kwargs):

    base = []
    base_collaborator = []
    base_follower = []
    base_tag = []
    base_field = []
    base_metric = []

    for row in data["tickets"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        # base
        base = _rebase(
            page,
            base,
            {
                "ticket_id": _int(row["id"]),
                "external_id": _str(row["external_id"]),
                "created_at": _ts(row["created_at"]),
                "updated_at": _ts(row["updated_at"]),
                "type": _str(row["type"]),
                "priority": _str(row["priority"]),
                "status": _str(row["status"]),
                "requester_id": _int(row["requester_id"]),
                "submitter_id": _int(row["submitter_id"]),
                "assignee_id": _int(row["assignee_id"]),
                "org_id": _int(row["organization_id"]),
                "group_id": _int(row["group_id"]),
                "brand_id": _int(row["brand_id"]),
                "form_id": _int(
                    row["ticket_form_id"] if "ticket_form_id" in row else None
                ),
                "channel": _str(
                    row["via"]["channel"]
                    if "via" in row and row["via"] != None and "channel" in row["via"]
                    else None
                ),
                "description": _str(row["description"]),
                "description_masked": _str_mask(row["description"]),
                "problem_id": _int(row["problem_id"]),
                "subject": _str(row["subject"]),
                "subject_masked": _str_mask(row["subject"]),
                "url": _str(row["url"]),
                "raw_subject": _str(row["raw_subject"]),
                "recipient": _str(row["recipient"]),
                "forum_topic_id": _str(row["forum_topic_id"]),
                "has_incidents": _bool(row["has_incidents"]),
                "is_public": _bool(row["is_public"]),
                "satisfaction_probability": _str(
                    row.get("satisfaction_probability", None)
                ),
                "allow_channelback": _bool(row["allow_channelback"]),
                "allow_attachments": _bool(row["allow_attachments"]),
                "generated_timestamp": _int(row["generated_timestamp"]),
                "email_cc_ids": _str(row["email_cc_ids"]),
                "sharing_agreement_ids": _str(row["sharing_agreement_ids"]),
                "followup_ids": _str(row["followup_ids"]),
                "due_at": _ts(row["due_at"]),
                "satisfaction_rating_score": _str(
                    row["satisfaction_rating"]["score"]
                    if "satisfaction_rating" in row
                    and row["satisfaction_rating"] != None
                    and "score" in row["satisfaction_rating"]
                    and row["satisfaction_rating"]["score"] != "unoffered"
                    and "tags" in row
                    and row["tags"] != None
                    and "csat_sent" in row["tags"]
                    else None
                ),
                "satisfaction_rating_id": _str(
                    row["satisfaction_rating"]["id"]
                    if "satisfaction_rating" in row
                    and row["satisfaction_rating"] != None
                    and "id" in row["satisfaction_rating"]
                    else None
                ),
                "satisfaction_rating_comment": _str(
                    row["satisfaction_rating"]["comment"]
                    if "satisfaction_rating" in row
                    and row["satisfaction_rating"] != None
                    and "comment" in row["satisfaction_rating"]
                    else None
                ),
                "satisfaction_rating_reason": _str(
                    row["satisfaction_rating"]["reason"]
                    if "satisfaction_rating" in row
                    and row["satisfaction_rating"] != None
                    and "reason" in row["satisfaction_rating"]
                    else None
                ),
                "satisfaction_rating_reason_id": _str(
                    row["satisfaction_rating"]["reason_id"]
                    if "satisfaction_rating" in row
                    and row["satisfaction_rating"] != None
                    and "reason_id" in row["satisfaction_rating"]
                    else None
                ),
                "created_part": _str(row["created_at"][:10]),
            },
        )

        # collaborator
        if "collaborator_ids" in row:
            for row_collaborator in row["collaborator_ids"]:
                base_collaborator = _rebase(
                    page,
                    base_collaborator,
                    {
                        "ticket_id": _int(row["id"]),
                        "collaborator_id": _int(row_collaborator),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

        # follower
        if "follower_ids" in row:
            for row_follower in row["follower_ids"]:
                base_follower = _rebase(
                    page,
                    base_follower,
                    {
                        "ticket_id": _int(row["id"]),
                        "follower_id": _int(row_follower),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

        # tag
        if "tags" in row:
            for row_tag in row["tags"]:
                base_tag = _rebase(
                    page,
                    base_tag,
                    {
                        "ticket_id": _int(row["id"]),
                        "tag": _str(row_tag),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

        # field
        if "fields" in row:
            for row_field in row["fields"]:
                if row_field["value"] != None:
                    base_field = _rebase(
                        page,
                        base_field,
                        {
                            "ticket_id": _int(row["id"]),
                            "field_id": _str(row_field["id"]),
                            "value": _str(row_field["value"]),
                            "created_part": _str(row["created_at"][:10]),
                        },
                    )

    for row in data["metric_sets"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        base_metric = _rebase(
            page,
            base_metric,
            {
                "metric_id": _int(row["id"]),
                "ticket_id": _int(row["ticket_id"]),
                "created_at": _ts(row["created_at"]),
                "solved_at": _ts(row["solved_at"]),
                "url": _str(row["url"]),
                "created_part": _str(row["created_at"][:10]),
            },
        )

    return [
        ["ticket", base],
        ["ticket_collaborator", base_collaborator],
        ["ticket_follower", base_follower],
        ["ticket_tag", base_tag],
        ["ticket_field", base_field],
        ["ticket_metric", base_metric],
    ]


def _extract_incremental_ticket_events(data, page, kwargs):

    base_comment = []
    base_attachment = []

    for row in data["ticket_events"]:

        if kwargs["load_type"] == "historical" and not (
            kwargs["start_date"] <= _ts(row["created_at"]) <= kwargs["end_date"]
        ):
            continue

        for row_child in row["child_events"]:
            if "type" in row_child and row_child["type"] == "Comment":

                # comment
                base_comment = _rebase(
                    page,
                    base_comment,
                    {
                        "ticket_id": _int(row["ticket_id"]),
                        "comment_id": _int(row["id"]),
                        "created_at": _ts(row_child["created_at"]),
                        "body": _str(row_child["body"]),
                        "body_masked": _str_mask(row_child["body"]),
                        "is_public": _bool(row_child["public"]),
                        "author_id": _int(row_child["author_id"]),
                        "created_part": _str(row["created_at"][:10]),
                    },
                )

                if "attachments" in row_child and row_child["attachments"] != None:
                    for row_attach in row_child["attachments"]:

                        # attachment
                        base_attachment = _rebase(
                            page,
                            base_attachment,
                            {
                                "ticket_id": _int(row["ticket_id"]),
                                "comment_id": _int(row["id"]),
                                "attachment_id": _int(row_attach["id"]),
                                "content_url": _str(row_attach["content_url"]),
                                "content_type": _str(row_attach["content_type"]),
                                "filename": _str(row_attach["file_name"]),
                                "is_public": _bool(row_child["public"]),
                                "created_part": _str(row["created_at"][:10]),
                            },
                        )

    return [["comment", base_comment], ["attachment", base_attachment]]


###############################################################################


def _get_switch_base(start_time):

    return {
        "organization_fields": {
            "url": "/api/v2/organization_fields.json",
            "func": _extract_organization_fields,
        },
        "incremental_organizations": {
            "url": f"/api/v2/incremental/organizations.json?start_time={start_time}",
            "func": _extract_incremental_organizations,
        },
        "user_fields": {
            "url": "/api/v2/user_fields.json",
            "func": _extract_user_fields,
        },
        "incremental_users": {
            "url": f"/api/v2/incremental/users.json?include=identities&start_time={start_time}",
            "func": _extract_incremental_users,
        },
        "groups": {"url": "/api/v2/groups.json", "func": _extract_groups},
        "group_memberships": {
            "url": "/api/v2/group_memberships.json",
            "func": _extract_group_memberships,
        },
        "ticket_forms": {
            "url": "/api/v2/ticket_forms.json",
            "func": _extract_ticket_forms,
        },
        "ticket_fields": {
            "url": "/api/v2/ticket_fields.json",
            "func": _extract_ticket_fields,
        },
        "incremental_tickets": {
            "url": f"/api/v2/incremental/tickets.json?include=metric_sets&start_time={start_time}",
            "func": _extract_incremental_tickets,
        },
        "incremental_ticket_events": {
            "url": f"/api/v2/incremental/ticket_events.json?include=comment_events&start_time={start_time}",
            "func": _extract_incremental_ticket_events,
        },
    }


###############################################################################


def _persist(extract, mode, kwargs):

    base, data = extract

    schema = zendesk_model.get_extract_schema(base)

    extract_bucket = kwargs["extract_bucket"]
    extract_prefix = kwargs["extract_prefix"]
    extract_path = f"s3://{extract_bucket}/{extract_prefix}{base}/"

    df = spark.createDataFrame(data, schema)

    df.write.mode(mode).parquet(extract_path)
    _info(f"Parquet {extract_path} {mode} successfully.")


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

    zendesk_headers = {
        "Content-Type": "application/json",
        "Authorization": kwargs["api_auth"],
    }

    # First URL
    url = "https://" + kwargs["api_url"] + select_base["url"]

    # Starts capturing API data
    mode = "overwrite"
    pages_count = 0
    while url != None:

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
                    raise Exception(f"Abort after {errors_max} requests error.")

                sleep_seconds = kwargs["sleep_default"]
                _info(f"Retry in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                continue

            data = resp.json()

        if data:
            func = select_base["func"]
            extracts = func(data, pages_count, kwargs)
            for extract in extracts:
                _persist(extract, mode, kwargs)
            pages_count += 1

        url = None
        mode = "append"
        if (
            ("end_of_stream" not in data or not data["end_of_stream"])
            and "next_page" in data
            and data["next_page"] != None
            and pages_count < pages_max
        ):
            url = data["next_page"]


###############################################################################


def main():

    _info("Start")

    app_args = json.loads(job_args.zendesk_args)

    extract_strategy = str(app_args["extract_strategy"]).lower()

    # delta
    if extract_strategy == "full":
        load_type = "full"
        start_date = None
        end_date = None
    elif "days" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            days=int(extract_strategy.replace("days", ""))
        )
        end_date = None
    elif "day" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            days=int(extract_strategy.replace("day", ""))
        )
        end_date = None
    elif "hours" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            hours=int(extract_strategy.replace("hours", ""))
        )
        end_date = None
    elif "hour" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            hours=int(extract_strategy.replace("hour", ""))
        )
        end_date = None
    elif "minutes" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            minutes=int(extract_strategy.replace("minutes", ""))
        )
        end_date = None
    elif "minute" in extract_strategy:
        load_type = "delta"
        start_date = datetime.today() - timedelta(
            minutes=int(extract_strategy.replace("minute", ""))
        )
        end_date = None
    elif ";" in extract_strategy or ":" in extract_strategy:
        load_type = "historical"
        start_date = _ts(extract_strategy.split(";")[0])
        end_date = _ts(extract_strategy.split(";")[1])
    else:
        raise Exception(
            "Invalid Extract Strategy. Possible values: Full, X days/hours/minutes, 2020-01-01T00:00:00Z;2021-01-01T23:59:59Z."
        )

    if start_date:
        start_timestamp = str(
            int(start_date.replace(tzinfo=timezone.utc).astimezone(tz=None).timestamp())
        )
    else:
        start_timestamp = 0

    kwargs = {
        "spark": spark,
        "extract_bucket": app_args["extract_bucket"],
        "extract_prefix": app_args["extract_prefix"]
        if app_args["extract_prefix"][-1:] == "/"
        else app_args["extract_prefix"] + "/",
        "api_url": app_args["api_url"],
        "api_auth": app_args["api_auth"],
        "pages_max": float("inf")
        if app_args["pages_max"] == "none"
        else int(app_args["pages_max"]),
        "errors_max": app_args["errors_max"],
        "sleep_default": app_args["sleep_default"],
        "load_type": load_type,
        "start_date": start_date,
        "end_date": end_date,
        "start_timestamp": start_timestamp,
    }

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
