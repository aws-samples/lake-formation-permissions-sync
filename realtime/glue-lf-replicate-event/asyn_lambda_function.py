"""
Summary of Execution:

This Lambda function processes unprocessed Glue/Lake Formation events from DynamoDB in strict order. It acquires a distributed lock to prevent concurrent execution, then queries for unprocessed events. For each event:
1. It validates event order against a checkpoint.
2. Converts the event to boto3 parameters and calls the appropriate AWS Glue/Lake Formation API.
3. If successful, it atomically marks the event as processed and updates the checkpoint using a DynamoDB transaction.
4. The process repeats until all events are processed or an error occurs.
The function ensures exactly-once, ordered processing and prevents duplicate or out-of-order event handling.
"""
# from urllib import response
import boto3
import json
import ast
import os
import time
import random
from decimal import Decimal
from configparser import ConfigParser
from botocore.errorfactory import ClientError
from botocore.exceptions import BotoCoreError, EndpointConnectionError
from cloudtrail_to_boto3 import cloudtail_to_boto3_converter
from boto3.dynamodb.conditions import Key

# Reserved EventId used for checkpoint tracking
CHECKPOINT_ID = "__CHECKPOINT__"
LOCK_ID = "__PROCESS_LOCK__"

# ---- Helpers to extract bucket name ----
def get_s3_table_target_bucket_name(table_location):
    s3_table_target_bucket_name = table_location.replace("s3://", "").split("/")[0]
    return s3_table_target_bucket_name[:-1] if s3_table_target_bucket_name.endswith('/') else s3_table_target_bucket_name

# ---- Load Config ----
def get_config(s3_config_bucket, s3_config_file):
    s3 = boto3.client('s3')
    body_content = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)['Body'].read().decode('utf-8')
    config = ConfigParser()
    config.read_string(body_content)
    return config

config_file_bucket = os.environ['config_file_bucket']
config_file_key = os.environ['config_file_key']
config = get_config(config_file_bucket, config_file_key)
SOURCE_REGION = config['AwsDataCatalog']['source_region']
TARGET_REGION = config['AwsDataCatalog']['destination_region']
table_s3_mapping = ast.literal_eval(config.get('AwsDataCatalog', 'S3BucketMapping'))

print (f"Config loaded: Source={SOURCE_REGION}, Target={TARGET_REGION}, S3Mapping={table_s3_mapping}")

# ---- AWS Clients ----
session = boto3.Session()
dynamodb = boto3.resource('dynamodb', region_name=SOURCE_REGION)
table = dynamodb.Table("glue_lf_events")
ddb_client = boto3.client('dynamodb', region_name=SOURCE_REGION)
glue_client = session.client('glue', region_name=TARGET_REGION)
lf_client = session.client('lakeformation', region_name=TARGET_REGION)

# ---- Retry Configuration ----
RETRYABLE_ERROR_CODES = {
    "Throttling", "ThrottlingException", "TooManyRequestsException",
    "RequestLimitExceeded", "ServiceUnavailable", "SlowDown",
    "InternalServiceException", "OperationTimeoutException",
    "ConcurrentModificationException", "ProvisionedThroughputExceededException",
    "LimitExceededException",
    "ConcurrentModificationException"
}

MAX_ATTEMPTS = 8
BASE_BACKOFF = 0.5
MAX_BACKOFF = 8.0

# ---------------------------
# Distributed Lock Logic
# ---------------------------
def acquire_lock():
    """
    Acquire a distributed lock using DynamoDB.
    Prevents multiple processes from running simultaneously.
    """
    try:
        table.put_item(
            Item={
                "EventId": LOCK_ID,
                "LockOwner": "PROCESS_1",
                "LockTime": int(time.time())
            },
            ConditionExpression="attribute_not_exists(EventId) OR LockTime < :expiry",
            ExpressionAttributeValues={
                ":expiry": int(time.time()) - 300  # lock expires after 5 minutes
            }
        )
        print("Lock acquired successfully.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("Another process is already running. Exiting.")
            return False
        raise

def release_lock():
    """
    Release the distributed lock.
    """
    table.delete_item(Key={"EventId": LOCK_ID})
    print("Lock released.")

# ---------------------------
# Checkpoint & Order Logic
# ---------------------------
def validate_next_event(event_time):
    """
    Ensure that this event is the exact next event after the checkpoint.
    """
    checkpoint = table.get_item(Key={"EventId": CHECKPOINT_ID}).get('Item', {})

    if not checkpoint:
        print("No checkpoint found, treating as first event.")
        return

    # Convert both values to integers
    incoming_time = int(event_time)
    last_time = int(checkpoint.get('LastEventTime', 0))

    print(f"Checkpoint before processing: {checkpoint}")
    print(f"Comparing incoming_time={incoming_time} with last_time={last_time}")

    if incoming_time <= last_time:
        raise RuntimeError(
            f"Out of order event! Incoming: {incoming_time}, Checkpoint: {last_time}"
        )

def debug_checkpoint():
    checkpoint = table.get_item(Key={"EventId": CHECKPOINT_ID}).get('Item', {})
    print("Current checkpoint item:", checkpoint)
    return checkpoint

# ---------------------------
# Atomic Transaction Logic
# ---------------------------
def mark_event_and_update_checkpoint(event_id, new_event_time):
    """
    Atomically mark an event as processed AND update the checkpoint in one DynamoDB transaction.
    """
    try:
        ddb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": "glue_lf_events",
                        "Key": {"EventId": {"S": event_id}},
                        "UpdateExpression": "SET #p = :y",
                        "ExpressionAttributeNames": {"#p": "Processed"},
                        "ExpressionAttributeValues": {
                            ":y": {"S": "Y"},
                            ":n": {"S": "N"}
                        },
                        "ConditionExpression": "#p = :n"
                    }
                },
                {
                    "Update": {
                        "TableName": "glue_lf_events",
                        "Key": {"EventId": {"S": CHECKPOINT_ID}},
                        "UpdateExpression": "SET LastEventTime = :t, LastEventId = :eid, #p = :p",
                        "ExpressionAttributeNames": {"#p": "Processed"},
                        "ExpressionAttributeValues": {
                            ":t": {"N": str(int(new_event_time))},  # Send as number
                            ":eid": {"S": event_id},
                            ":p": {"S": "Y"}
                        },
                        "ConditionExpression": "attribute_not_exists(LastEventTime) OR LastEventTime < :t"
                    }
                }
            ]
        )
        print(f"Event {event_id} marked as processed and checkpoint updated atomically.")
    except ClientError as e:
        print("Transaction failed. Debugging info:")
        debug_checkpoint()
        raise


def mark_event_processed_only(event_id):
    """Mark a single event as processed (Processed = Y) if currently N."""
    table.update_item(
        Key={'EventId': event_id},
        UpdateExpression="SET #p = :y",
        ConditionExpression="#p = :n",
        ExpressionAttributeNames={"#p": "Processed"},
        ExpressionAttributeValues={":y": "Y", ":n": "N"}
    )
    print(f"Event {event_id} marked processed (no checkpoint update).")


def _sleep_with_jitter(base, attempt):
    cap = min(MAX_BACKOFF, base * (2 ** attempt))
    time.sleep(random.uniform(0, cap))

def call_with_retries(client, method_name, **kwargs):
    """Generic AWS SDK call wrapper with exponential backoff for transient failures."""
    method = getattr(client, method_name)
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            return method(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in RETRYABLE_ERROR_CODES:
                print(f"[{method_name}] Retryable error {code}, attempt {attempt+1}/{MAX_ATTEMPTS}")
                last_err = e
                _sleep_with_jitter(BASE_BACKOFF, attempt)
                continue
            raise
        except (EndpointConnectionError, BotoCoreError) as e:
            print(f"[{method_name}] Network error {repr(e)}, attempt {attempt+1}/{MAX_ATTEMPTS}")
            last_err = e
            _sleep_with_jitter(BASE_BACKOFF, attempt)
            continue
    raise last_err if last_err else RuntimeError(f"{method_name} failed with no response")

# ---- Checkpoint Management ----
def get_checkpoint():
    resp = table.get_item(Key={"EventId": CHECKPOINT_ID})
    return resp.get("Item", {})

def update_checkpoint(new_event_time, new_event_id):
    """Atomically update checkpoint to enforce strict ordering."""
    table.update_item(
        Key={"EventId": CHECKPOINT_ID},
        UpdateExpression="SET LastEventTime = :t, LastEventId = :eid, Processed = :p",
        ConditionExpression="attribute_not_exists(LastEventTime) OR LastEventTime < :t",
        ExpressionAttributeValues={
            ":t": new_event_time,
            ":eid": new_event_id,
            ":p": "Y"
        }
    )
    print(f"Checkpoint advanced to {new_event_time} ({new_event_id})")

def validate_event_order(event):
    """Ensure this event is strictly later than the last checkpoint."""
    checkpoint = get_checkpoint()
    if checkpoint and 'LastEventTime' in checkpoint:
        def _to_int(v):
            if isinstance(v, Decimal):
                return int(v)
            try:
                return int(str(v))
            except (TypeError, ValueError):
                raise RuntimeError(f"Unable to interpret event time value {v!r} as integer")

        event_time = _to_int(event.get('EventTime'))
        last_time = _to_int(checkpoint['LastEventTime'])

        if event_time <= last_time:
            raise RuntimeError(
                f"Out-of-order event detected! "
                f"EventTime {event_time} <= Last checkpoint {last_time}"
            )

# ---- Normalization Helpers ----
def _normalize_table_input(boto3_parameters):
    # Remove VersionId if present to avoid Glue conflict
    if 'VersionId' in boto3_parameters:
        print(f"[DEBUG] Removing VersionId from boto3_parameters: {boto3_parameters['VersionId']}")
        boto3_parameters.pop('VersionId')

    # Normalize TableInput
    ti = boto3_parameters.get('TableInput', {})
    ti.pop('isRowFilteringEnabled', None)

    sd = ti.get('StorageDescriptor', {})
    
    # Ensure NumberOfBuckets is integer
    if 'NumberOfBuckets' in sd:
        sd['NumberOfBuckets'] = int(sd['NumberOfBuckets'] or 0)

    # Ensure Retention is integer
    if 'Retention' in ti:
        ti['Retention'] = int(ti['Retention'] or 0)

    # Map source S3 location to target S3 location
    if 'Location' in sd:
        src_bucket = get_s3_table_target_bucket_name(sd['Location'])
        if src_bucket in table_s3_mapping:
            tgt_bucket = table_s3_mapping[src_bucket]
            ti['StorageDescriptor']['Location'] = sd['Location'].replace(src_bucket, tgt_bucket)
            print(f"[DEBUG] Updated S3 Location from {src_bucket} to {tgt_bucket}")
            
def _normalize_partition_input_list(boto3_parameters):
    pil = boto3_parameters.get('PartitionInputList', [])
    if pil and 'StorageDescriptor' in pil[0]:
        sd = pil[0]['StorageDescriptor']
        if 'NumberOfBuckets' in sd:
            sd['NumberOfBuckets'] = int(sd['NumberOfBuckets'] or 0)

def _normalize_dls(boto3_parameters):
    dls = boto3_parameters.get('DataLakeSettings', {})
    for k in ('Parameters', 'whitelistedForExternalDataFiltering', 'disallowGrantOnIAMAllowedPrincipals'):
        dls.pop(k, None)

# ---- Core Event Processor ----
def process_event(event_name, boto3_parameters, event_id):
    """Run the correct Glue or LF action for this event."""
    response = None
    try:
        if event_name == 'CreateTable':
            _normalize_table_input(boto3_parameters)
            response = call_with_retries(glue_client, 'create_table', **boto3_parameters)

        elif event_name == "UpdateTable":
            _normalize_table_input(boto3_parameters)
            response = call_with_retries(glue_client, 'update_table', **boto3_parameters)

        elif event_name == "DeleteTable":
            response = call_with_retries(glue_client, 'delete_table', **boto3_parameters)

        elif event_name == "CreateDatabase":
            response = call_with_retries(glue_client, 'create_database', **boto3_parameters)

        elif event_name == "UpdateDatabase":
            response = call_with_retries(glue_client, 'update_database', **boto3_parameters)

        elif event_name == "DeleteDatabase":
            response = call_with_retries(glue_client, 'delete_database', **boto3_parameters)

        elif event_name == "RegisterResource":
            response = call_with_retries(lf_client, 'register_resource', **boto3_parameters)

        elif event_name == "DeregisterResource":
            response = call_with_retries(lf_client, 'deregister_resource', **boto3_parameters)

        elif event_name == "PutDataLakeSettings":
            _normalize_dls(boto3_parameters)
            response = call_with_retries(lf_client, 'put_data_lake_settings', **boto3_parameters)

        elif event_name == "CreateLFTag":
            response = call_with_retries(lf_client, 'create_lf_tag', **boto3_parameters)

        elif event_name == "UpdateLFTag":
            response = call_with_retries(lf_client, 'update_lf_tag', **boto3_parameters)

        elif event_name == "DeleteLFTag":
            response = call_with_retries(lf_client, 'delete_lf_tag', **boto3_parameters)

        elif event_name == "AddLFTagsToResource":
            response = call_with_retries(lf_client, 'add_lf_tags_to_resource', **boto3_parameters)

        elif event_name == "GrantPermissions":
            response = call_with_retries(lf_client, 'grant_permissions', **boto3_parameters)

        elif event_name == "RevokePermissions":
            response = call_with_retries(lf_client, 'revoke_permissions', **boto3_parameters)

        elif event_name == "BatchGrantPermissions":
            response = call_with_retries(lf_client, 'batch_grant_permissions', **boto3_parameters)

        elif event_name == "BatchRevokePermissions":
            response = call_with_retries(lf_client, 'batch_revoke_permissions', **boto3_parameters)

        elif event_name == "BatchCreatePartition":
            _normalize_partition_input_list(boto3_parameters)
            response = call_with_retries(glue_client, 'batch_create_partition', **boto3_parameters)

        else:
            raise RuntimeError(f"Unsupported event type {event_name}")

        if response is not None and response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200 and not response.get('Failures', []):
            return "Y", response
        else:
            return "N", response

    except ClientError as e:
        code = e.response['Error']['Code']
        print(f"ClientError: {code} on event {event_id}")
        if code in ("AlreadyExistsException", "EntityNotFoundException", "InvalidInputException"):
            # Safe to treat as success and advance
            return "Y", response
        raise

def run_event_processing():
    """Process unprocessed events grouping ties by EventTime.

    For each smallest unprocessed EventTime:
      - fetch all unprocessed events with that EventTime,
      - sort them by the nested metadata_location parameter,
      - replay each event in that order and mark it processed (without
        advancing the checkpoint),
      - after the whole group is applied advance the checkpoint once.
    """
    processed_count = 0

    while True:
        # Find the next EventTime to process (smallest EventTime among unprocessed)
        head = table.query(
            IndexName="Processed-EventTime-index",
            KeyConditionExpression=Key('Processed').eq('N'),
            ScanIndexForward=True,
            Limit=1
        ).get('Items', [])

        if not head:
            print("No unprocessed events found.")
            break

        next_time = head[0]['EventTime']
        print(f"Next EventTime to process: {next_time}")

        # Fetch all events with that EventTime (handle pagination)
        events = []
        last_evaluated = None
        while True:
            q = {
                'IndexName': 'Processed-EventTime-index',
                'KeyConditionExpression': Key('Processed').eq('N') & Key('EventTime').eq(next_time),
                'ScanIndexForward': True,
                'ProjectionExpression': 'EventId, CloudTrailEvent',
                'Limit': 100
            }
            if last_evaluated:
                q['ExclusiveStartKey'] = last_evaluated

            resp = table.query(**q)
            events.extend(resp.get('Items', []))
            last_evaluated = resp.get('LastEvaluatedKey')
            if not last_evaluated:
                break

        if not events:
            # No unprocessed events at that timestamp (race), continue
            continue

        # Extract metadata_location for sorting; fallback to EventId to be deterministic
        def extract_metadata_location(item):
            """Return metadata_location if table is ICEBERG, else None."""
            try:
                ct = json.loads(item.get('CloudTrailEvent', '{}'))
            except json.JSONDecodeError:
                print(f"[ERROR] EventId={item.get('EventId', 'UNKNOWN')} has invalid JSON in CloudTrailEvent")
                return None
            except Exception as e:
                print(f"[ERROR] EventId={item.get('EventId', 'UNKNOWN')} unexpected error decoding JSON: {e}")
                return None

            table_params = (
                ct.get('requestParameters', {})
                .get('tableInput', {})
                .get('parameters', {})
            )

            if table_params.get('table_type') == 'ICEBERG':
                return table_params.get('metadata_location')
            return None

        def sort_key(event):
            """Sort by databaseName (asc), tableInput.name (asc), versionId (desc)."""
            try:
                # Parse the CloudTrailEvent JSON
                ct_event = json.loads(event["CloudTrailEvent"])
                params = ct_event.get("requestParameters", {})

                db_name = params.get("databaseName", "")
                table_name = params.get("tableInput", {}).get("name", "")
                version_raw = params.get("versionId", "0")

                # Convert versionId to integer
                try:
                    version_id = int(version_raw)
                except (ValueError, TypeError):
                    version_id = 0

                # Invert version_id to achieve descending order
                return (db_name, table_name, -version_id)

            except Exception as e:
                print(f"[ERROR] Failed to extract key for EventId={event.get('EventId')}: {e}")
                return ("", "", 0)


        events_sorted = sorted(events, key=sort_key)

        # Replay in order; mark each event processed
        for idx, event in enumerate(events_sorted):
            event_id = event['EventId']
            print (f"Processing EventId={event_id} at index {idx} of {len(events_sorted)} for EventTime={next_time}")
            time.sleep(1)
            try:
                full_event = table.get_item(Key={'EventId': event_id}).get('Item', {})
                if not full_event:
                    raise RuntimeError(f"Event {event_id} disappeared before processing")

                if idx == 0:
                    validate_event_order(full_event)

                cw_request = json.loads(full_event['CloudTrailEvent'])
                event_name = cw_request['eventName']
                
                boto3_parameters = cloudtail_to_boto3_converter(cw_request['requestParameters'])

                print(f"Processing EventId={event_id}, Name={event_name} with parameters {boto3_parameters}")
                status, _ = process_event(event_name, boto3_parameters, event_id)

                if status == 'Y':
                    if idx == len(events_sorted) - 1:
                        # Final event in this timestamp group: atomically mark and advance checkpoint
                        mark_event_and_update_checkpoint(event_id, next_time)
                    else:
                        # Earlier events: mark processed only
                        mark_event_processed_only(event_id)
                    processed_count += 1
                else:
                    raise RuntimeError(f"Event {event_id} not marked processed, halting.")

            except Exception as e:
                print(f"Error processing event {event_id}: {repr(e)}")
                # Fail-fast: stop immediately to avoid gaps
                raise

        # Loop again to find the next timestamp

    return {
        'body': json.dumps(f'Process completed. {processed_count} events processed successfully.')
    }


# ---- Lambda Handler ----

def lambda_handler(event, context):
    if not acquire_lock():
        return {"body": json.dumps("Another process is already running. Exiting.")}

    try:
        result = run_event_processing()
        return result
    finally:
        release_lock()

if __name__ == "__main__":
    lambda_handler({}, {})