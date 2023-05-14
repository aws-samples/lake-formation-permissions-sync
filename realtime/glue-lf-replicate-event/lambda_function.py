import boto3
import json
import ast
import os
from configparser import ConfigParser
from botocore.errorfactory import ClientError
from cloudtrail_to_boto3 import cloudtail_to_boto3_converter
from boto3.dynamodb.conditions import Key


def get_s3_table_target_bucket_name(table_location):
    s3_table_target_bucket_name = table_location.replace("s3://", "").split("/")[0]
    s3_table_target_bucket_name = (s3_table_target_bucket_name[:-1] if s3_table_target_bucket_name.endswith('/') else s3_table_target_bucket_name)
    return s3_table_target_bucket_name


def get_config(s3_config_bucket,s3_config_file ):
    s3 = boto3.client('s3')
    body_content = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)['Body'].read().decode('utf-8')
    config = ConfigParser()
    config.read_string(body_content)
    return config

config_file_bucket = os.environ['config_file_bucket']
config_file_key = os.environ['config_file_key']
config = get_config(config_file_bucket,config_file_key)
SOURCE_REGION = config['AwsDataCatalog']['source_region']
TARGET_REGION = config['AwsDataCatalog']['destination_region']
table_s3_mapping = ast.literal_eval(config.get('AwsDataCatalog','S3BucketMapping'))


session = boto3.Session()
dynamodb = boto3.resource('dynamodb', region_name=SOURCE_REGION)
ct_client = session.client('cloudtrail', region_name=SOURCE_REGION)
glue_client = session.client('glue', region_name=TARGET_REGION)
lf_client = session.client('lakeformation', region_name=TARGET_REGION)

lake_formation_exceptions = lf_client.exceptions

AlreadyExistsException = lake_formation_exceptions.AlreadyExistsException
InternalServiceException = lake_formation_exceptions.InternalServiceException
InvalidInputException = lake_formation_exceptions.InvalidInputException
OperationTimeoutException = lake_formation_exceptions.OperationTimeoutException

table = dynamodb.Table("glue_lf_events")

def event_processed(response, event_id):
    if response is not None and response['ResponseMetadata']['HTTPStatusCode'] == 200 and not response.get('Failures',[]):
        response = table.update_item(
            Key={'EventId': event_id},
            AttributeUpdates={
                'Processed': {
                    'Value': 'Y'
                },
            })
        print (f"Record for {event_id} updated to Y")
        return "Y"
    else:
        print (f"Error : Unable to update due to failure reason {response.get('Failures',[])}")
    return "N"

def lambda_handler(event, context):
    response = table.query(
        IndexName='Processed-EventTime-index',
        KeyConditionExpression=Key('Processed').eq('N')
    )

    for k in response['Items']:
        try:
            event_id = k['EventId']
            print(f"Processing event id {event_id}")
            response = table.get_item(
                Key={
                    'EventId': k['EventId']
                },
            )
            event_name = response['Item']['EventName']
            event_source = response['Item']['EventSource']
            cw_request = json.loads(response['Item']['CloudTrailEvent'])
            cloudtrail_event = cw_request['requestParameters']
            print(f"{event_source} => {event_name} => {cloudtrail_event}")
            boto3_parameters = cloudtail_to_boto3_converter(cloudtrail_event)
            print (f"Running call with Boto3 Parameters {boto3_parameters}")
            print (f"Now processing event id {event_id} for event => {event_name}")
            response = None
            record_processed_status = None
            if event_name == 'CreateTable':
                try:
                    boto3_parameters['TableInput'].pop('isRowFilteringEnabled', None)
                    boto3_parameters['TableInput']['StorageDescriptor']['NumberOfBuckets'] = int(boto3_parameters['TableInput']['StorageDescriptor']['NumberOfBuckets'])
                    boto3_parameters['TableInput']['Retention'] = int(boto3_parameters['TableInput']['Retention'])
                    s3_table_location = boto3_parameters['TableInput']['StorageDescriptor']['Location']
                    s3_table_bucket_name = get_s3_table_target_bucket_name(s3_table_location)
                    if s3_table_bucket_name in table_s3_mapping:
                        s3_table_target_bucket_name = table_s3_mapping[s3_table_bucket_name]
                        boto3_parameters['TableInput']['StorageDescriptor']['Location'] = boto3_parameters['TableInput']['StorageDescriptor']['Location'].replace(s3_table_bucket_name, s3_table_target_bucket_name)
                    response = glue_client.create_table(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Table Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == "CreateDatabase":

                try:
                    response = glue_client.create_database(**boto3_parameters)
                    print(f"Response for {event_id} => {response}")
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == "DeleteDatabase":
                try:
                    response = glue_client.delete_database(**boto3_parameters)
                    print(f"Response for {event_id} => {response}")
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    elif err.response['Error']['Code'] == "EntityNotFoundException":
                        print("Database does not exist")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == "UpdateDatabase":
                try:
                    response = glue_client.update_database(**boto3_parameters)
                    print(f"Response for {event_id} => {response}")
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    elif err.response['Error']['Code'] == "EntityNotFoundException":
                        print("Database does not exist")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == "UpdateTable":
                try:
                    boto3_parameters['TableInput'].pop('isRowFilteringEnabled', None)
                    boto3_parameters['TableInput']['StorageDescriptor']['NumberOfBuckets'] = int(
                    boto3_parameters['TableInput']['StorageDescriptor']['NumberOfBuckets'])
                    boto3_parameters['TableInput']['Retention'] = int(boto3_parameters['TableInput']['Retention'])
                    s3_table_location = boto3_parameters['TableInput']['StorageDescriptor']['Location']
                    s3_table_bucket_name = get_s3_table_target_bucket_name(s3_table_location)
                    if s3_table_bucket_name in table_s3_mapping:
                        s3_table_target_bucket_name = table_s3_mapping[s3_table_bucket_name]
                        boto3_parameters['TableInput']['StorageDescriptor']['Location'] = boto3_parameters['TableInput']['StorageDescriptor']['Location'].replace(s3_table_bucket_name, s3_table_target_bucket_name)
                    response = glue_client.update_table(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == "DeleteTable":
                try:
                    response = glue_client.delete_table(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Table Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'RegisterResource':
                try:
                    response = lf_client.register_resource(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'DeregisterResource':
                try:
                    response = lf_client.deregister_resource(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'PutDataLakeSettings':
                try:
                    boto3_parameters['DataLakeSettings'].pop('Parameters', None)
                    boto3_parameters['DataLakeSettings'].pop('whitelistedForExternalDataFiltering', None)
                    boto3_parameters['DataLakeSettings'].pop('disallowGrantOnIAMAllowedPrincipals', None)
                    response = lf_client.put_data_lake_settings(**boto3_parameters)
                    print(f"Response for {event_id} => {response}")
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AccessDeniedException":
                        print("PutDataLakeSettings AccessDeniedException exception occurred")
                        print(err)
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'CreateLFTag':
                print("CreateLFTag LF event:")
                try:
                    response = lf_client.create_lf_tag(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    print(err)
                    if err.response['Error']['Code'] == "AccessDeniedException":
                        print("CreateLFTag AccessDeniedException exception")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'DeleteLFTag':
                print("DeleteLFTag LF event:")
                try:
                    response = lf_client.delete_lf_tag(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AccessDeniedException":
                        print("DeleteLFTag AccessDeniedException exception")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'UpdateLFTag':
                try:
                    response = lf_client.update_lf_tag(**boto3_parameters)
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AccessDeniedException":
                        print("UpdateLFTag AccessDeniedException exception")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'AddLFTagsToResource':
                try:
                    response = lf_client.add_lf_tags_to_resource(**boto3_parameters)
                    print (f"Response for {event_id} => {response}")
                    record_processed_status = event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Database Already Exists")
                        record_processed_status = event_processed(response, k['EventId'])
                    else:
                        raise err

            elif event_name == 'BatchGrantPermissions':
                try:
                    print(boto3_parameters)
                    response = lf_client.batch_grant_permissions(**boto3_parameters)
                    print(response)
                    event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "InvalidInputException":
                        print("BatchGrantPermissions InvalidInputException occurred!")
                        event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'BatchRevokePermissions':
                try:
                    print(boto3_parameters)
                    response = lf_client.batch_revoke_permissions(**boto3_parameters)
                    print(response)
                    event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "InvalidInputException":
                        print("BatchRevokePermissions InvalidInputException occurred!")
                        event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'RevokePermissions':
                try:
                    response = lf_client.revoke_permissions(**boto3_parameters)
                    print(response)
                    event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "EntityNotFoundException":
                        print("RevokePermissions EntityNotFoundException exception")
                        event_processed(response, k['EventId'])
                    elif err.response['Error']['Code'] == "InvalidInputException":
                        print(err)
                        event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'GrantPermissions':
                try:
                    response = lf_client.grant_permissions(**boto3_parameters)
                    event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "EntityNotFoundException":
                        print("GrantPermissions EntityNotFoundException exception")
                        event_processed(response, k['EventId'])
                    else:
                        raise err
            elif event_name == 'BatchCreatePartition':
                boto3_parameters['PartitionInputList'][0]['StorageDescriptor']['NumberOfBuckets'] = int(
                    boto3_parameters['PartitionInputList'][0]['StorageDescriptor']['NumberOfBuckets'])
                print(boto3_parameters)
                try:
                    response = glue_client.batch_create_partition(**boto3_parameters)
                    event_processed(response, k['EventId'])
                except ClientError as err:
                    if err.response['Error']['Code'] == "AlreadyExistsException":
                        print("Partitions Already Exists")
                        event_processed(response, k['EventId'])
                    else:
                        raise err

            print(f"Response for {event_id} => {response} with processed status {record_processed_status}")
        except Exception as e:
            print (f"Received exception {e}")

    return {
        'body': json.dumps('Process completed!')
    }


