import json
import boto3
import os
from botocore.errorfactory import ClientError
from configparser import ConfigParser

def get_config(s3_config_bucket,s3_config_file ):
    s3 = boto3.client('s3')
    body_content = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)['Body'].read().decode('utf-8')
    config = ConfigParser()
    config.read_string(body_content)
    #config.read("glue_config.conf")
    return config

config_file_bucket = os.environ['config_file_bucket']
config_file_key = os.environ['config_file_key']
config = get_config(config_file_bucket,config_file_key)

target_region = config['AwsDataCatalog']['destination_region']
lakeformation_iam_role = os.environ['LAMBDA_IAM_ROLE']

lf_client = boto3.client('lakeformation', region_name=target_region)

lake_formation_exceptions = lf_client.exceptions
InternalServiceException = lake_formation_exceptions.InternalServiceException
InvalidInputException = lake_formation_exceptions.InvalidInputException

def lambda_handler(event, context):
    try:
        response = lf_client.get_data_lake_settings()
        response['DataLakeSettings']['DataLakeAdmins'].append({'DataLakePrincipalIdentifier': lakeformation_iam_role})
        print(response['DataLakeSettings'])
        new_response = lf_client.put_data_lake_settings(DataLakeSettings=response['DataLakeSettings'])
        print(new_response)
    except ClientError as err:
        if err.response['Error']['Code'] == "InvalidInputException":
            print("PutDataLakeSettings InvalidInputException exception occurred")
            print(err)
        else:
            raise err
    return {
        'statusCode': 200,
        'body': json.dumps('Set LF Administrator in target region!')
    }
