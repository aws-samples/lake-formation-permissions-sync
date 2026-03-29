"""Lambda: Pull Glue/LakeFormation events from CloudTrail into DynamoDB.

Polls CloudTrail for recent Glue and Lake Formation API events,
filters for successful mutations, and inserts them into DynamoDB
for downstream processing by the replicate-event Lambda.

Improvements over the original:
- Lazy client initialization (no module-level failures)
- Structured logging
- Uses TABLE_NAME env var instead of hardcoded name
- Cleaner event filtering with a set instead of inline list
"""

import datetime
import json
import logging
import os

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Events we track for replication
TRACKED_EVENTS = {
    "BatchRevokePermissions",
    "BatchGrantPermissions",
    "CreateLFTag",
    "DeleteLFTag",
    "UpdateLFTag",
    "GrantPermissions",
    "RevokePermissions",
    "CreateDatabase",
    "DeleteDatabase",
    "UpdateDatabase",
    "CreateTable",
    "BatchCreatePartition",
    "CreatePartition",
    "UpdateTable",
    "DeleteTable",
    "RegisterResource",
    "DeregisterResource",
    "PutDataLakeSettings",
    "AddLFTagsToResource",
    "CreateDataCellsFilter",
}

EVENT_SOURCES = [
    "glue.amazonaws.com",
    "lakeformation.amazonaws.com",
]


class DatetimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


# ─── Lazy initialization ───
_config = None
_ct_client = None
_table = None


def _get_config():
    global _config
    if _config is None:
        from shared.config_loader import get_config

        _config = get_config()
    return _config


def _get_cloudtrail_client():
    global _ct_client
    if _ct_client is None:
        config = _get_config()
        source_region = config["AwsDataCatalog"]["source_region"]
        _ct_client = boto3.Session().client("cloudtrail", region_name=source_region)
    return _ct_client


def _get_dynamodb_table():
    global _table
    if _table is None:
        table_name = os.environ.get("TABLE_NAME", "glue_lf_events")
        _table = boto3.resource("dynamodb").Table(table_name)
    return _table


def is_request_successful(event):
    """Check if a CloudTrail event represents a successful API call."""
    cloud_trail_event = json.loads(event["CloudTrailEvent"])

    # If there's an error code, the request failed
    if "errorCode" in cloud_trail_event:
        return False

    response_elements = cloud_trail_event.get("responseElements")

    # Some successful operations return null responseElements
    if response_elements is None and cloud_trail_event.get("errorCode") is None:
        return True

    # Check for empty failures list (batch operations)
    if response_elements is not None and not response_elements.get("failures", []):
        return True

    return False


def lambda_handler(event, context):
    """Poll CloudTrail and insert new events into DynamoDB."""
    config = _get_config()
    lookup_hours = int(config["AwsDataCatalog"]["cloudtrail_lookup_hour_duration"])
    start_time = datetime.datetime.now() - datetime.timedelta(hours=lookup_hours)

    ct_client = _get_cloudtrail_client()
    table = _get_dynamodb_table()
    paginator = ct_client.get_paginator("lookup_events")

    logger.info("Pulling events since %s", start_time)

    stats = {"inserted": 0, "skipped": 0, "duplicate": 0}

    for event_source in EVENT_SOURCES:
        logger.info("Scanning event source: %s", event_source)

        page_iterator = paginator.paginate(
            LookupAttributes=[
                {"AttributeKey": "EventSource", "AttributeValue": event_source},
            ],
            PaginationConfig={"PageSize": 50},
            StartTime=start_time,
        )

        for page in page_iterator:
            for ct_event in page["Events"]:
                event_name = ct_event["EventName"]

                if event_name not in TRACKED_EVENTS:
                    stats["skipped"] += 1
                    continue

                if not is_request_successful(ct_event):
                    logger.debug("Skipping failed event %s: %s", ct_event["EventId"], event_name)
                    stats["skipped"] += 1
                    continue

                logger.info("Inserting event %s: %s", ct_event["EventId"], event_name)

                try:
                    ct_event["EventTime"] = ct_event["EventTime"].strftime("%Y%m%d%H%M%S")
                    ct_event["Processed"] = "N"

                    table.put_item(
                        Item=ct_event,
                        ConditionExpression="attribute_not_exists(EventId)",
                    )
                    stats["inserted"] += 1

                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        logger.debug("Duplicate event %s, skipping", ct_event["EventId"])
                        stats["duplicate"] += 1
                    else:
                        logger.error("Error inserting event %s: %s", ct_event["EventId"], e)
                        raise

    logger.info("CloudTrail pull complete: %s", stats)

    return {
        "statusCode": 200,
        "body": json.dumps(stats),
    }
