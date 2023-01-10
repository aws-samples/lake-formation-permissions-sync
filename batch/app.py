#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.glue_restore_job import GlueRestoreOnDemandStack
from stacks.add_role_as_lf_admin import AddRoleAsLFAdminStack

app = cdk.App()
glueStack = GlueRestoreOnDemandStack(app, "GlueRestoreOnDemandStack")
glue_role_arn = glueStack.glue_role_arn

target_lf_region = app.node.try_get_context("target_region")
target_env = cdk.Environment(account=app.account, region=target_lf_region)
lfAddRoleAsAdminStack = AddRoleAsLFAdminStack(app,"GlueRoleAsLFAdminStack",env=target_env,role=glue_role_arn)

app.synth()
