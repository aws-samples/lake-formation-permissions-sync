import json
import boto3
import datetime
import botocore
import os
from configparser import ConfigParser


def get_config(s3_config_bucket,s3_config_file):
    s3 = boto3.client('s3')
    body_content = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)['Body'].read().decode('utf-8')
    config = ConfigParser()
    config.read_string(body_content)
    #config.read("glue_config.conf")
    return config


dynamodb = boto3.resource('dynamodb')

lake_formation_table = dynamodb.Table('glue_lf_events')

config_file_bucket = os.environ['config_file_bucket']
config_file_key = os.environ['config_file_key']
config = get_config(config_file_bucket,config_file_key)

SOURCE_REGION = config['AwsDataCatalog']['source_region']
LOOKUP_HOUR_DURATION = int(config['AwsDataCatalog']['cloudtrail_lookup_hour_duration'])

session = boto3.Session()
ct_client = session.client('cloudtrail', region_name=SOURCE_REGION)

paginator = ct_client.get_paginator('lookup_events')

start_time = datetime.datetime.now() - datetime.timedelta(hours=LOOKUP_HOUR_DURATION)

print(f"Start time {start_time}")

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)



def is_request_successful(event):
    #event['EventTime'] = event['EventTime'].strftime("%Y%m%d%H%M%S")
    cloud_trail_event = json.loads(event['CloudTrailEvent'])
    if 'errorCode' in cloud_trail_event.keys():
        if cloud_trail_event.get('errorCode') == 'AlreadyExistsException':
            return False
    if 'responseElements' in cloud_trail_event:
        print ("Now checking response element")
        if cloud_trail_event['responseElements'] is None and cloud_trail_event.get('errorCode',None) is None:
            return True
        if cloud_trail_event['responseElements'] is not None and not cloud_trail_event['responseElements'].get("failures",[]):
            return True
    return False


def lambda_handler(event, context):
    StartingToken = None
    for attribte_value in ["glue.amazonaws.com", "lakeformation.amazonaws.com"]:
        print(f"Attribute value {attribte_value}")
        page_iterator = paginator.paginate(
            LookupAttributes=[
                {'AttributeKey': 'EventSource', 'AttributeValue': attribte_value},
            ],
            PaginationConfig={'PageSize': 50, 'StartingToken': StartingToken},
            StartTime=start_time
        )
        for page in page_iterator:
            for event in page["Events"]:
                print(f" {event['EventName']}  =>  {event}")
                if event['EventName'] in ['BatchRevokePermissions', 'BatchGrantPermissions', 'CreateLFTag','DeleteLFTag', 'UpdateLFTag',
                                          'GrantPermissions', 'RevokePermissions', 'CreateDatabase', 'DeleteDatabase','UpdateDatabase',
                                          'CreateTable','BatchCreatePartition','UpdateTable','DeleteTable', 'RegisterResource','DeregisterResource', 'PutDataLakeSettings',
                                          'AddLFTagsToResource','CreateDataCellsFilter'] and is_request_successful(event):

                    print (f"Inserting record in DynamoDB table for event id {event['EventId']}")
                    try:
                        event['EventTime'] = event['EventTime'].strftime("%Y%m%d%H%M%S")
                        event['Processed'] = 'N'
                        response = lake_formation_table.put_item(
                            Item=event,
                            ConditionExpression='attribute_not_exists(EventId)'
                        )
                        print(response)
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                            print(e)
                else:
                    print(f"Skipping record insert in DynamoDB table for event id {event['EventId']}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing Completed')
    }

