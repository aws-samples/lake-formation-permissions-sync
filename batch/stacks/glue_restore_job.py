from aws_cdk import (
    Stack,
    aws_iam,
    aws_s3,
    aws_glue,
    aws_s3_deployment
)
from constructs import Construct

class GlueRestoreOnDemandStack(Stack):
    glue_role_arn = ''
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        # Create a role for use by the glue python script
        glue_role = aws_iam.Role(
            self, 'glue_role_id2323',
            role_name = 'LFRestoreOnDemandGlueRole',
            assumed_by=aws_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                              aws_iam.ManagedPolicy.from_aws_managed_policy_name('AWSLakeFormationDataAdmin')]
        )
        self.glue_role_arn = glue_role.role_arn
        config_bucket = self.node.try_get_context("config_bucket_name") 
        if not config_bucket:
            config_bucket = '' 
        s3arn_prefix = 'arn:aws:s3:::'
        glue_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:GetObjectVersion",
                    "s3:GetLifecycleConfiguration"],
            resources=[s3arn_prefix+config_bucket, s3arn_prefix+config_bucket+'/*']
        ))
        backup_bucket = self.node.try_get_context("backup_bucket_name") 
        if not backup_bucket:
            backup_bucket = '' 
        glue_role.add_to_policy(aws_iam.PolicyStatement(
            actions=["s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetLifecycleConfiguration",
                    "s3:PutLifecycleConfiguration",
                    "s3:DeleteObject"],
            resources=[s3arn_prefix+backup_bucket, s3arn_prefix+backup_bucket+'/*']
        ))    
        # Create an S3 bucket to hold the python script file
        script_bucket = aws_s3.Bucket(self,self.node.id,encryption=aws_s3.BucketEncryption.KMS_MANAGED,block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL)  
        script_bucket.grant_read(glue_role)
        # Upload the job script code to S3
        aws_s3_deployment.BucketDeployment(self,"DeployGluePythonScriptFile",destination_bucket=script_bucket,
            sources=[aws_s3_deployment.Source.asset("./script")])
        # Create a glue job with the role and the script code
        config_filename = self.node.try_get_context("config_file_name") 
        if not config_filename:
            config_filename = 'glue_config.conf'         
        aws_glue.CfnJob(self, "LFRestoreOnDemandGlueJob",role=glue_role.role_arn,
                command=aws_glue.CfnJob.JobCommandProperty(
                    name="pythonshell",
                    python_version="3.9",
                    
                    script_location="s3://"+script_bucket.bucket_name+"/app.py"
                ),
                default_arguments={
                    '--CONFIG_BUCKET':config_bucket,
                    '--CONFIG_FILE_KEY': config_filename
                },
                max_capacity=1
            )
        




