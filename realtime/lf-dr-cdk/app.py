#!/usr/bin/env python3
import os

import aws_cdk as cdk

from lf_dr_cdk.lf_dr_cdk_stack import LfDrCdkStack
from lf_dr_cdk.lf_dr_cdk_admin_stack import LfDrCdkLFAdminStack


app = cdk.App()
lambdaStack = LfDrCdkStack(app, "LfDrCdkStack")

lambda_role_arn = lambdaStack.lambda_role_arn
target_lf_region = app.node.try_get_context("target_region")
target_env = cdk.Environment(account=app.account, region=target_lf_region)
lfAddRoleAsAdminStack = LfDrCdkLFAdminStack(app,"LfDrCdkLFAdminStack",env=target_env,role=lambda_role_arn)
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=cdk.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html

lfAddRoleAsAdminStack.add_dependency(lambdaStack)

    

app.synth()
