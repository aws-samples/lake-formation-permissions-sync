from aws_cdk import (
    Stack,
    aws_lakeformation,
    CfnOutput
)
from constructs import Construct

class LfDrCdkLFAdminStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *,
        env=None, stack_name=None, tags=None, role='') -> None:
        super().__init__(scope, construct_id, env=env, stack_name=stack_name, tags=tags)

        CfnOutput(self, "RoleArn", value=role)
        # The code that defines your stack goes here
        # Add the input role as admin to the DataLake Formation instance
        aws_lakeformation.CfnDataLakeSettings(self, "MyCfnDataLakeSettings",
            admins=[aws_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                data_lake_principal_identifier=role
            )]
        )
