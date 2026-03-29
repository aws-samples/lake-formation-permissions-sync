"""Lambda: Replicate Glue/LakeFormation events to target region.

Reads unprocessed events from DynamoDB, converts CloudTrail parameters
to boto3 format, and replays the API calls against the target region.

Improvements over the original:
- Dispatch table replaces 260-line if/elif chain
- Proper pagination for DynamoDB queries
- response=None bug fixed in AlreadyExists handlers
- Structured logging with event context
- Idempotent error handling (AlreadyExists, EntityNotFound) handled uniformly
"""

import ast
import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.errorfactory import ClientError
from cloudtrail_to_boto3 import cloudtrail_to_boto3_converter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Idempotent exceptions: these mean the operation effectively succeeded ───
IDEMPOTENT_ERRORS = {
    "AlreadyExistsException",
    "EntityNotFoundException",
}

PERMISSIONS_IDEMPOTENT_ERRORS = {
    "EntityNotFoundException",
    "InvalidInputException",
    "AccessDeniedException",
}


# ─── Lazy-initialized clients (avoid module-level failures) ───
_clients = {}
_config = None
_table_s3_mapping = None


def _get_config():
    global _config, _table_s3_mapping
    if _config is None:
        from shared.config_loader import get_config

        _config = get_config()
        _table_s3_mapping = ast.literal_eval(_config.get("AwsDataCatalog", "S3BucketMapping"))
    return _config


def _get_client(name):
    if name not in _clients:
        config = _get_config()
        source_region = config["AwsDataCatalog"]["source_region"]
        target_region = config["AwsDataCatalog"]["destination_region"]
        session = boto3.Session()
        _clients["dynamodb"] = boto3.resource("dynamodb", region_name=source_region)
        _clients["glue"] = session.client("glue", region_name=target_region)
        _clients["glue_source"] = session.client("glue", region_name=source_region)
        _clients["lakeformation"] = session.client("lakeformation", region_name=target_region)
    return _clients[name]


def _get_table():
    table_name = os.environ.get("TABLE_NAME", "glue_lf_events")
    return _get_client("dynamodb").Table(table_name)


# ─── Pre-processors: clean up parameters before API call ───


def _get_s3_table_target_bucket_name(table_location):
    """Extract bucket name from an S3 location URI."""
    bucket = table_location.replace("s3://", "").split("/")[0]
    return bucket.rstrip("/")


def _remap_single_location(location):
    """Remap a single S3 URI using the table_s3_mapping. Returns remapped URI or original."""
    if not location:
        return location
    source_bucket = _get_s3_table_target_bucket_name(location)
    if source_bucket in _table_s3_mapping:
        return location.replace(source_bucket, _table_s3_mapping[source_bucket])
    return location


def _remap_s3_location(params):
    """Replace source S3 bucket with target bucket in all table locations:
    - StorageDescriptor.Location (Hive data path)
    - StorageDescriptor.AdditionalLocations (multi-location tables)
    - Parameters.metadata_location (Iceberg metadata pointer)
    - Parameters.previous_metadata_location (Iceberg previous metadata)
    """
    _get_config()  # ensure _table_s3_mapping is loaded
    table_input = params.get("TableInput", {})

    # --- StorageDescriptor.Location (Hive / general) ---
    sd = table_input.get("StorageDescriptor", {})
    if sd.get("Location"):
        sd["Location"] = _remap_single_location(sd["Location"])

    # --- StorageDescriptor.AdditionalLocations ---
    additional = sd.get("AdditionalLocations", [])
    if additional:
        sd["AdditionalLocations"] = [_remap_single_location(loc) for loc in additional]

    # --- Iceberg: metadata_location and previous_metadata_location ---
    parameters = table_input.get("Parameters", {})
    for key in ("metadata_location", "previous_metadata_location"):
        if parameters.get(key):
            parameters[key] = _remap_single_location(parameters[key])


def preprocess_table(params):
    """Clean up table parameters for Create/Update table calls."""
    table_input = params.get("TableInput", {})
    table_input.pop("isRowFilteringEnabled", None)
    sd = table_input.get("StorageDescriptor", {})
    if "NumberOfBuckets" in sd:
        sd["NumberOfBuckets"] = int(sd["NumberOfBuckets"])
    if "Retention" in table_input:
        table_input["Retention"] = int(table_input["Retention"])
    _remap_s3_location(params)
    return params


def preprocess_data_lake_settings(params):
    """Remove unsupported fields from PutDataLakeSettings."""
    settings = params.get("DataLakeSettings", {})
    for field in ("Parameters", "whitelistedForExternalDataFiltering", "disallowGrantOnIAMAllowedPrincipals", "setSourceIdentity"):
        settings.pop(field, None)
    return params


def _remap_partition_location(sd):
    """Remap S3 locations in a partition's StorageDescriptor (if mapped)."""
    _get_config()  # ensure _table_s3_mapping is loaded
    if sd.get("Location"):
        sd["Location"] = _remap_single_location(sd["Location"])
    additional = sd.get("AdditionalLocations", [])
    if additional:
        sd["AdditionalLocations"] = [_remap_single_location(loc) for loc in additional]


def preprocess_batch_create_partition(params):
    """Fix numeric types and remap S3 locations in partition input list."""
    for partition in params.get("PartitionInputList", []):
        sd = partition.get("StorageDescriptor", {})
        if "NumberOfBuckets" in sd:
            sd["NumberOfBuckets"] = int(sd["NumberOfBuckets"])
        _remap_partition_location(sd)
    return params


def preprocess_create_partition(params):
    """Fix numeric types and remap S3 location in single partition input."""
    partition = params.get("PartitionInput", {})
    sd = partition.get("StorageDescriptor", {})
    if "NumberOfBuckets" in sd:
        sd["NumberOfBuckets"] = int(sd["NumberOfBuckets"])
    _remap_partition_location(sd)
    return params


# ─── Dispatch table: maps event names to (client, method, preprocessor, idempotent_errors) ───

EVENT_DISPATCH = {
    # Glue Catalog events
    "CreateTable": ("glue", "create_table", preprocess_table, IDEMPOTENT_ERRORS),
    "UpdateTable": ("glue", "update_table", preprocess_table, IDEMPOTENT_ERRORS),
    "DeleteTable": ("glue", "delete_table", None, IDEMPOTENT_ERRORS),
    "CreateDatabase": ("glue", "create_database", None, IDEMPOTENT_ERRORS),
    "UpdateDatabase": ("glue", "update_database", None, IDEMPOTENT_ERRORS),
    "DeleteDatabase": ("glue", "delete_database", None, IDEMPOTENT_ERRORS),
    "BatchCreatePartition": ("glue", "batch_create_partition", preprocess_batch_create_partition, IDEMPOTENT_ERRORS),
    "CreatePartition": ("glue", "create_partition", preprocess_create_partition, IDEMPOTENT_ERRORS),
    # Lake Formation events
    "RegisterResource": ("lakeformation", "register_resource", None, IDEMPOTENT_ERRORS),
    "DeregisterResource": ("lakeformation", "deregister_resource", None, IDEMPOTENT_ERRORS),
    "PutDataLakeSettings": (
        "lakeformation",
        "put_data_lake_settings",
        preprocess_data_lake_settings,
        PERMISSIONS_IDEMPOTENT_ERRORS,
    ),
    "CreateLFTag": ("lakeformation", "create_lf_tag", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "DeleteLFTag": ("lakeformation", "delete_lf_tag", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "UpdateLFTag": ("lakeformation", "update_lf_tag", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "AddLFTagsToResource": ("lakeformation", "add_lf_tags_to_resource", None, IDEMPOTENT_ERRORS),
    # Permission events
    "BatchGrantPermissions": ("lakeformation", "batch_grant_permissions", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "BatchRevokePermissions": ("lakeformation", "batch_revoke_permissions", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "GrantPermissions": ("lakeformation", "grant_permissions", None, PERMISSIONS_IDEMPOTENT_ERRORS),
    "RevokePermissions": ("lakeformation", "revoke_permissions", None, PERMISSIONS_IDEMPOTENT_ERRORS),
}


CHECKPOINT_PK = "CHECKPOINT"


def _get_ddb_client():
    """Get a low-level DynamoDB client (needed for TransactWriteItems)."""
    config = _get_config()
    source_region = config["AwsDataCatalog"]["source_region"]
    if "ddb_client" not in _clients:
        _clients["ddb_client"] = boto3.client("dynamodb", region_name=source_region)
    return _clients["ddb_client"]


def mark_event_and_update_checkpoint(event_id, event_time):
    """Atomically mark an event as processed AND advance the checkpoint.

    Uses a high-water-mark approach: only moves the checkpoint forward.
    If the incoming event_time is older than the current checkpoint,
    the event is still marked processed but the checkpoint stays put.
    This avoids the ConditionalCheckFailed error when CloudTrail events
    arrive out of order.
    """
    table_name = os.environ.get("TABLE_NAME", "glue_lf_events")
    ddb = _get_ddb_client()

    # Build the transaction: always mark event processed
    transact_items = [
        {
            "Update": {
                "TableName": table_name,
                "Key": {"EventId": {"S": event_id}},
                "UpdateExpression": "SET #P = :val",
                "ExpressionAttributeNames": {"#P": "Processed"},
                "ExpressionAttributeValues": {":val": {"S": "Y"}},
            }
        },
    ]

    # Conditionally advance checkpoint — only if new time > current time
    # Uses attribute_not_exists (first-ever checkpoint) OR LastEventTime < new value
    if event_time:
        transact_items.append(
            {
                "Update": {
                    "TableName": table_name,
                    "Key": {"EventId": {"S": CHECKPOINT_PK}},
                    "UpdateExpression": "SET LastEventTime = :t",
                    "ConditionExpression": ("attribute_not_exists(LastEventTime) OR LastEventTime < :t"),
                    "ExpressionAttributeValues": {":t": {"S": str(event_time)}},
                }
            }
        )

    try:
        ddb.transact_write_items(TransactItems=transact_items)
        logger.info("Event %s marked processed, checkpoint at %s", event_id, event_time)
        return "Y"
    except ClientError as e:
        if e.response["Error"]["Code"] == "TransactionCanceledException":
            reasons = e.response.get("CancellationReasons", [])
            # Check if only the checkpoint condition failed (event row succeeded)
            if (
                len(reasons) >= 2
                and reasons[0].get("Code") == "None"
                and reasons[1].get("Code") == "ConditionalCheckFailed"
            ):
                # Event time is older than checkpoint — this is fine for out-of-order events
                # Just mark the event as processed without advancing the checkpoint
                logger.info(
                    "Event %s: timestamp %s is older than checkpoint, "
                    "marking processed without advancing checkpoint",
                    event_id,
                    event_time,
                )
                _mark_processed_simple(event_id, table_name, ddb)
                return "Y"
            logger.error("Transaction cancelled for event %s: %s", event_id, reasons)
            raise
        raise


def _mark_processed_simple(event_id, table_name, ddb):
    """Simple non-transactional mark as processed (fallback for out-of-order events)."""
    ddb.update_item(
        TableName=table_name,
        Key={"EventId": {"S": event_id}},
        UpdateExpression="SET #P = :val",
        ExpressionAttributeNames={"#P": "Processed"},
        ExpressionAttributeValues={":val": {"S": "Y"}},
    )


def _mark_processed(table, event_id, response, event_time=None):
    """Mark an event as processed in DynamoDB.

    Checks that the API response indicates success before marking.
    Uses transactional checkpoint if event_time is provided.
    Returns 'Y' if marked, 'N' otherwise.
    """
    if response is None:
        logger.warning("Cannot mark %s processed: response is None", event_id)
        return "N"

    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    failures = response.get("Failures", [])

    if status_code == 200 and not failures:
        if event_time:
            return mark_event_and_update_checkpoint(event_id, event_time)
        else:
            table.update_item(
                Key={"EventId": event_id},
                UpdateExpression="SET #P = :val",
                ExpressionAttributeNames={"#P": "Processed"},
                ExpressionAttributeValues={":val": "Y"},
            )
            logger.info("Event %s marked as processed", event_id)
            return "Y"

    logger.error("Event %s had failures: %s", event_id, failures)
    return "N"


def _mark_idempotent_success(table, event_id, error_code, event_name, event_time=None):
    """Mark an event as processed when we hit an idempotent error."""
    logger.info(
        "Event %s (%s): idempotent error %s — marking as processed",
        event_id,
        event_name,
        error_code,
    )
    if event_time:
        return mark_event_and_update_checkpoint(event_id, event_time)
    else:
        table.update_item(
            Key={"EventId": event_id},
            UpdateExpression="SET #P = :val",
            ExpressionAttributeNames={"#P": "Processed"},
            ExpressionAttributeValues={":val": "Y"},
        )
    return "Y"


def _is_omitted_event(cloudtrail_event):
    """Check if CloudTrail omitted the requestParameters due to size limits."""
    return cloudtrail_event.get("omitted") == "true" or cloudtrail_event.get("reason") == "requestParameters too large"


# Fields to strip when converting a source get_table response into a TableInput
_TABLE_NON_INPUT_FIELDS = {
    "CatalogId",
    "DatabaseName",
    "CreateTime",
    "UpdateTime",
    "CreatedBy",
    "IsRegisteredWithLakeFormation",
    "VersionId",
    "LastAccessTime",
}


def _fetch_table_from_source(cloudtrail_event_full):
    """When requestParameters are omitted, fetch the table from source Glue
    and reconstruct the parameters for Create/UpdateTable.

    cloudtrail_event_full is the full CloudTrail event JSON (not just requestParameters).
    """
    resources = cloudtrail_event_full.get("resources", [])
    # Extract database and table from ARN: arn:aws:glue:region:account:table/db/table
    db_name = table_name = None
    for r in resources:
        arn = r.get("ARN", "")
        if ":table/" in arn:
            parts = arn.split(":table/")[-1].split("/")
            if len(parts) >= 2:
                db_name, table_name = parts[0], parts[1]
                break
        elif ":database/" in arn and db_name is None:
            db_name = arn.split(":database/")[-1]

    if not db_name or not table_name:
        logger.error("Cannot extract db/table from omitted event resources: %s", resources)
        return None

    source_glue = _get_client("glue_source")
    try:
        resp = source_glue.get_table(DatabaseName=db_name, Name=table_name)
        table_def = resp["Table"]
        for field in _TABLE_NON_INPUT_FIELDS:
            table_def.pop(field, None)
        return {
            "DatabaseName": db_name,
            "TableInput": table_def,
        }
    except ClientError as e:
        logger.error("Failed to fetch table %s.%s from source: %s", db_name, table_name, e)
        return None


def _process_event(
    table, event_id, event_name, event_source, cloudtrail_event, cloudtrail_event_full=None, event_time=None
):
    """Process a single event using the dispatch table."""
    if event_name not in EVENT_DISPATCH:
        logger.warning("Unsupported event type: %s (source: %s)", event_name, event_source)
        return None

    client_name, method_name, preprocessor, idempotent_errors = EVENT_DISPATCH[event_name]

    # --- Handle omitted requestParameters (CloudTrail >256KB limit) ---
    if _is_omitted_event(cloudtrail_event):
        if event_name in ("CreateTable", "UpdateTable") and cloudtrail_event_full:
            logger.warning(
                "Event %s: requestParameters omitted (size=%s). " "Fetching table definition from source Glue.",
                event_id,
                cloudtrail_event.get("originalSize", "?"),
            )
            boto3_params = _fetch_table_from_source(cloudtrail_event_full)
            if boto3_params is None:
                logger.error("Event %s: could not reconstruct params from source, skipping", event_id)
                return None
            # Still run the preprocessor for type fixes, S3 remapping, Iceberg fix
            if preprocessor:
                boto3_params = preprocessor(boto3_params)
        else:
            logger.warning(
                "Event %s (%s): requestParameters omitted and no fallback " "available. Skipping. Original size=%s",
                event_id,
                event_name,
                cloudtrail_event.get("originalSize", "?"),
            )
            return None
    else:
        # Normal path: convert CloudTrail params to boto3 format
        boto3_params = cloudtrail_to_boto3_converter(cloudtrail_event)

    logger.info("Processing %s with params: %s", event_name, json.dumps(boto3_params, default=str)[:500])

    # Apply any pre-processing
    if preprocessor:
        boto3_params = preprocessor(boto3_params)

    # Call the API
    client = _get_client(client_name)
    api_method = getattr(client, method_name)

    try:
        response = api_method(**boto3_params)
        return _mark_processed(table, event_id, response, event_time=event_time)
    except ClientError as err:
        error_code = err.response["Error"]["Code"]
        if error_code in idempotent_errors:
            return _mark_idempotent_success(table, event_id, error_code, event_name, event_time=event_time)
        logger.error("Event %s (%s) failed: %s", event_id, event_name, err)
        raise


def _get_unprocessed_events(table):
    """Query all unprocessed events with proper pagination."""
    all_items = []
    query_params = {
        "IndexName": "Processed-EventTime-index",
        "KeyConditionExpression": Key("Processed").eq("N"),
    }

    while True:
        response = table.query(**query_params)
        all_items.extend(response.get("Items", []))

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        query_params["ExclusiveStartKey"] = last_key

    return all_items


def lambda_handler(event, context):
    """Main handler: process all unprocessed events from DynamoDB."""
    table = _get_table()
    unprocessed = _get_unprocessed_events(table)
    logger.info("Found %d unprocessed events", len(unprocessed))

    results = {"processed": 0, "failed": 0, "skipped": 0}

    for item in unprocessed:
        event_id = item["EventId"]
        try:
            # Fetch the full event record
            full_record = table.get_item(Key={"EventId": event_id})
            record = full_record.get("Item", {})

            event_name = record.get("EventName")
            event_source = record.get("EventSource")
            event_time = record.get("EventTime")
            cw_event = json.loads(record.get("CloudTrailEvent", "{}"))
            ct_params = cw_event.get("requestParameters", {})

            logger.info("Processing event %s: %s from %s", event_id, event_name, event_source)

            status = _process_event(
                table,
                event_id,
                event_name,
                event_source,
                ct_params,
                cloudtrail_event_full=cw_event,
                event_time=event_time,
            )
            if status == "Y":
                results["processed"] += 1
            elif status is None:
                results["skipped"] += 1
            else:
                results["failed"] += 1

        except Exception as e:
            results["failed"] += 1
            logger.error("Exception processing event %s: %s", event_id, e, exc_info=True)

    logger.info("Processing complete: %s", results)
    return {
        "statusCode": 200,
        "body": json.dumps(results),
    }
