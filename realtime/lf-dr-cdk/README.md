# Near Real Time Replication of Glue Catalog objects and Lakeformation Permissions

This solution enables near real replication of AWS Glue Catalog and changes made through AWS Lake Formation. The source of this information is in CloudTrail. CloudTrail is by default enabled in an account and captures activity in an account, including AWS Glue and AWS Lake Formation.

These changes from the CloudTrail logs are read by a Lambda function which is triggered by a CloudWatch rule every minute ( configurable ). This data is stored in a Dynamo DB table with Event ID as the primary key. 

Since Cloudtrail logs are eventual consistent the lambda function uses this opportunity to read all Glue Catalog and Lake Formation generated Cloudtrail logs in the past hour and checks the existance of event id in the Dynamo DB table. A CloudTrail record is immutable and hence only the record that are not present gets inserted in the Dynamo DB table. The insert of a record in the Dynamo DB table is integrated with another Lambda function reading DynamoDB table stream. This stream data is then used to replicate the changes in the target region. Processed_Flag column in the DynamoDB table tracks the sucessful processing of a record. A new record is set with value 'N' and once a record is succcessfully processed this column is marked 'Y'. 


## Deployment Steps

### Prerequisites
- AWS CLI
- Node.js (>= 10.13.0, except for versions 13.0.0 - 13.6.0)
- AWS CDK


### CDK stack Deployment
At this point you can now start deployment of CDK code.
**Step 1: Navigate to location ```realtime/lf-dr-cdk/```. Initialize AWS CDK for first time only. If AWS CDK is already initialized then skip to Step 2.**

Option 1: With default AWS profile
```
cdk bootstrap --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all
```

Option 2: With passing AWS profile name:
```
cdk bootstrap --profile <aws_profile> --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all
```

Replace the config_file_key, config_file_bucket (s3 bucket where config file can be placed), target_region and eventbridge_schedule_min values with your preferred settings.

**Step 2: Deploy CDK stack with following command:**

Option 1: With default AWS profile
```
cdk deploy --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all
```

Option 2: With passing AWS profile name:
```
cdk deploy --profile <aws_profile> --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all 
```
Replace the config_file_key, config_file_bucket (s3 bucket where config file can be placed), target_region and eventbridge_schedule_min values with your preferred settings.

**Step 3: Clean up**

Option 1: With default AWS profile
```
cdk destroy --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all
```

Option 2: With passing AWS profile name:
```
cdk destroy --profile <aws_profile> --context config_file_key="config/glue-lf-config.conf" --context config_file_bucket="lf-metadata-artifact-bucket" --context target_region="us-west-2" --context eventbridge_schedule_min="1" --all
```

Replace the config_file_key, config_file_bucket (s3 bucket where config file can be placed), target_region and eventbridge_schedule_min values with your preferred settings.

You’ll be asked:
```
Are you sure you want to delete: CdkWorkshopStack (y/n)?
```
Hit “y” and you’ll see your stack being destroyed.

The bootstrapping stack created through ```cdk bootstrap``` still exists. If you plan on using the CDK in the future (we hope you do!) do not delete this stack.

If you would like to delete this stack, it will have to be done through the CloudFormation console. Head over to the CloudFormation console and delete the ```CDKToolkit``` stack. The S3 bucket created will be retained by default, so if you want to avoid any unexpected charges, be sure to head to the S3 console and empty + delete the bucket generated from bootstrapping.

### Useful commands

- ```cdk ls``` list all stacks in the app 
- ```cdk synth``` emits the synthesized CloudFormation template 
- ```cdk deploy``` deploy this stack to your default AWS account/region 
- ```cdk diff``` compare deployed stack with current state 
- ```cdk docs``` open CDK documentation







