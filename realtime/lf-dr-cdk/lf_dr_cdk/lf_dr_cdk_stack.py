from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    Aws,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment,
    triggers
)
from constructs import Construct
from aws_cdk.aws_lambda_event_sources import DynamoEventSource, SqsDlq


class LfDrCdkStack(Stack):
    lambda_role_arn = ''
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create dynamo table
        table = dynamodb.Table(
            self, "glue_lf_events",
            table_name="glue_lf_events",
            partition_key=dynamodb.Attribute(
                name="EventId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream= dynamodb.StreamViewType.NEW_IMAGE,
            removal_policy=RemovalPolicy.DESTROY,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,

        )

        table.add_global_secondary_index(
            index_name="Processed-index",
            partition_key=dynamodb.Attribute(
                name="Processed",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.KEYS_ONLY,
        )

        table.add_global_secondary_index(
            index_name="Processed-EventTime-index",
            partition_key=dynamodb.Attribute(
                name="Processed",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="EventTime",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.KEYS_ONLY,
        )

        table.add_global_secondary_index(
            index_name="EventTime-EventSource-index",
            partition_key=dynamodb.Attribute(
                name="EventTime",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="EventSource",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type = dynamodb.ProjectionType.KEYS_ONLY,
        )

        # table.add_global_secondary_index()
        lambda_role = iam.Role(self, "lf-dr-glue-lambda-iam",
                               role_name="glue-lambda-iam",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               description="Glue Lambda IAM role for Lake Formation DR",
                               path="/",
                               )

        lambda_cloudtrail_policy = iam.Policy(
            scope=self,
            id="CloudtrailPermissionPolicy"
        )

        lambda_cloudtrail_policy.add_statements(iam.PolicyStatement(
            actions=[
                "cloudtrail:LookupEvents"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                '*'
            ]
        ))

        lambda_role.attach_inline_policy(
            lambda_cloudtrail_policy
        )

        lambda_s3_get_policy = iam.Policy(
            scope=self,
            id="S3GetObjectPolicy"
        )

        lambda_s3_get_policy.add_statements(iam.PolicyStatement(
            actions=[
                "s3:GetObject"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                '*'
            ]
        ))

        lambda_role.attach_inline_policy(
            lambda_s3_get_policy
        )


        lambda_dynamo_policy = iam.Policy(
            scope=self,
            id="DynamoDBPermissionPolicy"
        )

        lambda_dynamo_policy.add_statements(iam.PolicyStatement(
            actions=[
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                table.table_arn, table.table_arn+'/index/Processed-EventTime-index'
            ]
        ))

        lambda_role.attach_inline_policy(
            lambda_dynamo_policy
        )

        lambda_lakeformation_policy = iam.Policy(
            scope=self,
            id="LakeformationPermissionPolicy"
        )
        lambda_lakeformation_policy.add_statements(iam.PolicyStatement(
            actions=[
                "iam:PutRolePolicy"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                 "arn:aws:iam::"+Aws.ACCOUNT_ID+":role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
            ]
        ))

        lambda_role.attach_inline_policy(
            lambda_lakeformation_policy
        )

        lambda_lakeformation_admin_policy = iam.Policy(
            scope=self,
            id="LakeformationAdminPermissionPolicy"
        )

        lambda_lakeformation_admin_policy.add_statements(iam.PolicyStatement(
            actions=[
                "lakeformation:*",
                "cloudtrail:DescribeTrails",
                "cloudtrail:LookupEvents",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:CreateDatabase",
                "glue:UpdateDatabase",
                "glue:DeleteDatabase",
                "glue:GetConnections",
                "glue:SearchTables",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:GetTables",
                "glue:GetWorkflow",
                "glue:ListWorkflows",
                "glue:BatchGetWorkflows",
                "glue:DeleteWorkflow",
                "glue:GetWorkflowRuns",
                "glue:StartWorkflowRun",
                "glue:GetWorkflow",
                "glue:BatchCreatePartition",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "iam:ListUsers",
                "iam:ListRoles",
                "iam:GetRole",
                "iam:GetRolePolicy"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                 "*"
            ]
        ))

        lambda_role.attach_inline_policy(
            lambda_lakeformation_admin_policy
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        # lambda_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess"))
        #
        # lambda_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin"))

        config_bucket = self.node.try_get_context("config_file_bucket")
        config_file_key = self.node.try_get_context("config_file_key")
        config_folder = config_file_key.split("/")
        # Upload the job script code to S3
        aws_s3_deployment.BucketDeployment(self, "DeployLambdaConfigFile", destination_bucket=s3.Bucket.from_bucket_name( \
            self,'imported-bucket-from-name',self.node.try_get_context("config_file_bucket")),
                                           destination_key_prefix=config_folder[0],
                                           sources=[aws_s3_deployment.Source.asset("./{}".format(config_folder[0]))])

        # Lambda functions
        glue_lf_cloudtrail_pull_new = lambda_.Function(
            self,
            "glue_lf_cloudtrail_pull_lambda",
            function_name="glue_lf_cloudtrail_pull_lambda",
            code=lambda_.Code.from_asset("./../glue-lf-cloudtrail-pull"),
            handler="lambda_function.lambda_handler",
            timeout=Duration.seconds(300),
            runtime=lambda_.Runtime.PYTHON_3_9,
            role=lambda_role,
            environment={
                "config_file_bucket": self.node.try_get_context("config_file_bucket"),
                "config_file_key": self.node.try_get_context("config_file_key"),
             }
        )

        # Add our configurable parameter in Minutes for scheduling rule for glue_lf_cloudtrail_pull_new Lambda (via a CloudWatch scheduled Role)
        rule = events.Rule(
            self,
            "LakeFormationSyncRule-InMinutes-new",
            schedule=events.Schedule.rate(Duration.minutes(int(self.node.try_get_context("eventbridge_schedule_min"))))
        )
        rule.add_target(targets.LambdaFunction(glue_lf_cloudtrail_pull_new))

        glue_lf_replicate_event = lambda_.Function(
            self,
            "glue_lf_replicate_event_lambda",
            function_name="glue_lf_replicate_event_lambda",
            code=lambda_.Code.from_asset("./../glue-lf-replicate-event"),
            handler="lambda_function.lambda_handler",
            timeout=Duration.seconds(900),
            runtime=lambda_.Runtime.PYTHON_3_9,
            role=lambda_role,
            environment={
                "config_file_bucket": self.node.try_get_context("config_file_bucket"),
                "config_file_key": self.node.try_get_context("config_file_key"),
             }
        )

        dead_letter_queue = sqs.Queue(self, "lfDRDeadLetterQueue")
        glue_lf_replicate_event.add_event_source(DynamoEventSource(table,
                                              starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                                              batch_size=1,
                                              bisect_batch_on_error=True,
                                              on_failure=SqsDlq(dead_letter_queue),
                                              retry_attempts=0
                                              ))

        table.grant_read_write_data(glue_lf_cloudtrail_pull_new)
        table.grant_read_write_data(glue_lf_replicate_event)
        glue_lf_cloudtrail_pull_new.add_environment('TABLE_NAME', table.table_name)
        self.lambda_role_arn = lambda_role.role_arn