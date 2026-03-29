from aws_cdk import (
    Aws,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import (
    aws_s3_deployment,
)
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import DynamoEventSource, SqsDlq
from constructs import Construct


    # Supported runtime strings → CDK Runtime objects
_LAMBDA_RUNTIMES = {
    "python3.10": lambda_.Runtime.PYTHON_3_10,
    "python3.11": lambda_.Runtime.PYTHON_3_11,
    "python3.12": lambda_.Runtime.PYTHON_3_12,
    "python3.13": lambda_.Runtime.PYTHON_3_13,
}
_DEFAULT_LAMBDA_RUNTIME = "python3.13"


class LfDrCdkStack(Stack):
    lambda_role_arn = ""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda runtime — configurable via CDK context (-c lambda_runtime=python3.12)
        runtime_key = self.node.try_get_context("lambda_runtime") or _DEFAULT_LAMBDA_RUNTIME
        lambda_runtime = _LAMBDA_RUNTIMES.get(runtime_key, lambda_.Runtime.PYTHON_3_13)

        # create dynamo table
        table = dynamodb.Table(
            self,
            "glue_lf_events",
            table_name="glue_lf_events",
            partition_key=dynamodb.Attribute(name="EventId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            removal_policy=RemovalPolicy.DESTROY,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
        )

        table.add_global_secondary_index(
            index_name="Processed-index",
            partition_key=dynamodb.Attribute(name="Processed", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.KEYS_ONLY,
        )

        table.add_global_secondary_index(
            index_name="Processed-EventTime-index",
            partition_key=dynamodb.Attribute(name="Processed", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="EventTime", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.KEYS_ONLY,
        )

        table.add_global_secondary_index(
            index_name="EventTime-EventSource-index",
            partition_key=dynamodb.Attribute(name="EventTime", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="EventSource", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.KEYS_ONLY,
        )

        # table.add_global_secondary_index()
        lambda_role = iam.Role(
            self,
            "lf-dr-glue-lambda-iam",
            role_name="glue-lambda-iam",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Glue Lambda IAM role for Lake Formation DR",
            path="/",
        )

        lambda_cloudtrail_policy = iam.Policy(scope=self, id="CloudtrailPermissionPolicy")

        lambda_cloudtrail_policy.add_statements(
            iam.PolicyStatement(actions=["cloudtrail:LookupEvents"], effect=iam.Effect.ALLOW, resources=["*"])
        )

        lambda_role.attach_inline_policy(lambda_cloudtrail_policy)

        config_bucket_name = self.node.try_get_context("config_file_bucket") or "*"
        lambda_s3_get_policy = iam.Policy(scope=self, id="S3GetObjectPolicy")

        lambda_s3_get_policy.add_statements(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                effect=iam.Effect.ALLOW,
                resources=[f"arn:aws:s3:::{config_bucket_name}/*"],
            )
        )

        lambda_role.attach_inline_policy(lambda_s3_get_policy)

        lambda_dynamo_policy = iam.Policy(scope=self, id="DynamoDBPermissionPolicy")

        lambda_dynamo_policy.add_statements(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:PutItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:GetItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:TransactWriteItems",
                ],
                effect=iam.Effect.ALLOW,
                resources=[table.table_arn, table.table_arn + "/index/Processed-EventTime-index"],
            )
        )

        lambda_role.attach_inline_policy(lambda_dynamo_policy)

        lambda_lakeformation_policy = iam.Policy(scope=self, id="LakeformationPermissionPolicy")
        lambda_lakeformation_policy.add_statements(
            iam.PolicyStatement(
                actions=["iam:PutRolePolicy"],
                effect=iam.Effect.ALLOW,
                resources=[
                    "arn:aws:iam::"
                    + Aws.ACCOUNT_ID
                    + ":role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
                ],
            )
        )

        lambda_role.attach_inline_policy(lambda_lakeformation_policy)

        lambda_lakeformation_admin_policy = iam.Policy(scope=self, id="LakeformationAdminPermissionPolicy")

        lambda_lakeformation_admin_policy.add_statements(
            iam.PolicyStatement(
                actions=[
                    # Lake Formation — scoped to actions actually used by the Lambdas
                    "lakeformation:GetDataLakeSettings",
                    "lakeformation:PutDataLakeSettings",
                    "lakeformation:GrantPermissions",
                    "lakeformation:RevokePermissions",
                    "lakeformation:BatchGrantPermissions",
                    "lakeformation:BatchRevokePermissions",
                    "lakeformation:ListPermissions",
                    "lakeformation:RegisterResource",
                    "lakeformation:DeregisterResource",
                    "lakeformation:GetResourceLFTags",
                    "lakeformation:AddLFTagsToResource",
                    "lakeformation:RemoveLFTagsFromResource",
                    "lakeformation:CreateLFTag",
                    "lakeformation:DeleteLFTag",
                    "lakeformation:UpdateLFTag",
                    "lakeformation:CreateDataCellsFilter",
                    # CloudTrail
                    "cloudtrail:DescribeTrails",
                    "cloudtrail:LookupEvents",
                    # Glue — scoped to actions actually used by the Lambdas
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetTableVersions",
                    "glue:GetPartitions",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                    # S3 — read-only for config and metadata
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListAllMyBuckets",
                    "s3:GetBucketAcl",
                    # IAM — read-only for LF admin verification
                    "iam:ListUsers",
                    "iam:ListRoles",
                    "iam:GetRole",
                    "iam:GetRolePolicy",
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        lambda_role.attach_inline_policy(lambda_lakeformation_admin_policy)

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # lambda_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess"))
        #
        # lambda_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin"))

        config_file_key = self.node.try_get_context("config_file_key")
        config_folder = config_file_key.split("/")
        # Upload the job script code to S3
        aws_s3_deployment.BucketDeployment(
            self,
            "DeployLambdaConfigFile",
            destination_bucket=s3.Bucket.from_bucket_name(
                self, "imported-bucket-from-name", self.node.try_get_context("config_file_bucket")
            ),
            destination_key_prefix=config_folder[0],
            sources=[aws_s3_deployment.Source.asset("./{}".format(config_folder[0]))],
        )

        # Lambda functions
        glue_lf_cloudtrail_pull_new = lambda_.Function(
            self,
            "glue_lf_cloudtrail_pull_lambda",
            function_name="glue_lf_cloudtrail_pull_lambda",
            code=lambda_.Code.from_asset("./../event_collector"),
            handler="lambda_function.lambda_handler",
            timeout=Duration.seconds(300),
            runtime=lambda_runtime,
            role=lambda_role,
            environment={
                "config_file_bucket": self.node.try_get_context("config_file_bucket"),
                "config_file_key": self.node.try_get_context("config_file_key"),
            },
        )

        # EventBridge schedule for CloudTrail pull Lambda
        schedule_min = int(self.node.try_get_context("eventbridge_schedule_min"))
        rule = events.Rule(
            self,
            "LakeFormationSyncRule-InMinutes-new",
            schedule=events.Schedule.rate(Duration.minutes(schedule_min)),
        )
        rule.add_target(targets.LambdaFunction(glue_lf_cloudtrail_pull_new))

        glue_lf_replicate_event = lambda_.Function(
            self,
            "glue_lf_replicate_event_lambda",
            function_name="glue_lf_replicate_event_lambda",
            code=lambda_.Code.from_asset("./../event_replicator"),
            handler="lambda_function.lambda_handler",
            timeout=Duration.seconds(900),
            runtime=lambda_runtime,
            role=lambda_role,
            environment={
                "config_file_bucket": self.node.try_get_context("config_file_bucket"),
                "config_file_key": self.node.try_get_context("config_file_key"),
            },
        )

        dead_letter_queue = sqs.Queue(self, "lfDRDeadLetterQueue")
        glue_lf_replicate_event.add_event_source(
            DynamoEventSource(
                table,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                batch_size=1,
                bisect_batch_on_error=True,
                on_failure=SqsDlq(dead_letter_queue),
                retry_attempts=0,
            )
        )

        table.grant_read_write_data(glue_lf_cloudtrail_pull_new)
        table.grant_read_write_data(glue_lf_replicate_event)
        glue_lf_cloudtrail_pull_new.add_environment("TABLE_NAME", table.table_name)
        self.lambda_role_arn = lambda_role.role_arn
