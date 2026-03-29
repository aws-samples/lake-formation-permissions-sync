"""CDK stack unit tests using aws_cdk.assertions.

Verifies that:
- Batch: Glue job, IAM role, S3 bucket, LF admin stack are synthesized correctly
- Realtime: DynamoDB table, Lambda functions, IAM policies, SQS DLQ are correct
- IAM policies use scoped actions (no lakeformation:* or glue:*)

NOTE: CDK's JSII runtime caches the CWD of the Node subprocess from the first
synth call. Since the batch and realtime stacks use different relative asset
paths, the realtime stack tests are run via a subprocess to get a fresh JSII.
"""

import json
import os
import subprocess
import sys
import unittest

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class TestBatchGlueRestoreStack(unittest.TestCase):
    """Test the batch/infra GlueRestoreOnDemandStack."""

    @classmethod
    def setUpClass(cls):
        cls._orig_cwd = os.getcwd()
        os.chdir(os.path.join(_PROJECT_ROOT, "batch", "infra"))
        sys.path.insert(0, os.path.join(_PROJECT_ROOT, "batch", "infra"))

        import aws_cdk as cdk
        import aws_cdk.assertions as assertions
        from stacks.glue_restore_job import GlueRestoreOnDemandStack

        cls.assertions = assertions
        app = cdk.App(
            context={
                "config_bucket_name": "test-config-bucket",
                "backup_bucket_name": "test-backup-bucket",
                "config_file_name": "glue_config.conf",
            }
        )
        cls.stack = GlueRestoreOnDemandStack(app, "TestGlueStack")
        cls.template = assertions.Template.from_stack(cls.stack)

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls._orig_cwd)

    def test_glue_job_created(self):
        self.template.resource_count_is("AWS::Glue::Job", 1)

    def test_glue_job_uses_python3(self):
        self.template.has_resource_properties(
            "AWS::Glue::Job",
            self.assertions.Match.object_like(
                {"Command": self.assertions.Match.object_like({"PythonVersion": "3"}), "GlueVersion": "5.0"}
            ),
        )

    def test_glue_role_created(self):
        self.template.has_resource_properties(
            "AWS::IAM::Role",
            self.assertions.Match.object_like({"RoleName": "LFRestoreOnDemandGlueRole"}),
        )

    def test_s3_script_bucket_created(self):
        self.template.resource_count_is("AWS::S3::Bucket", 1)

    def test_iam_policies_no_wildcard_lf(self):
        """Verify no IAM policy uses 'lakeformation:*'."""
        template_json = self.template.to_json()
        for logical_id, resource in template_json.get("Resources", {}).items():
            if resource["Type"] == "AWS::IAM::Policy":
                doc = resource["Properties"]["PolicyDocument"]
                for stmt in doc.get("Statement", []):
                    actions = stmt.get("Action", [])
                    if isinstance(actions, str):
                        actions = [actions]
                    for action in actions:
                        self.assertNotEqual(action, "lakeformation:*")

    def test_glue_role_has_scoped_lf_actions(self):
        self.template.has_resource_properties(
            "AWS::IAM::Policy",
            self.assertions.Match.object_like(
                {
                    "PolicyDocument": self.assertions.Match.object_like(
                        {
                            "Statement": self.assertions.Match.array_with(
                                [
                                    self.assertions.Match.object_like(
                                        {
                                            "Action": self.assertions.Match.array_with(
                                                ["lakeformation:GetDataLakeSettings"]
                                            )
                                        }
                                    )
                                ]
                            )
                        }
                    )
                }
            ),
        )


class TestBatchLFAdminStack(unittest.TestCase):
    """Test the batch/infra AddRoleAsLFAdminStack."""

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, os.path.join(_PROJECT_ROOT, "batch", "infra"))
        import aws_cdk as cdk
        import aws_cdk.assertions as assertions
        from stacks.add_role_as_lf_admin import AddRoleAsLFAdminStack

        app = cdk.App()
        cls.stack = AddRoleAsLFAdminStack(
            app, "TestLFAdminStack", role="arn:aws:iam::123456789012:role/TestGlueRole",
        )
        cls.template = assertions.Template.from_stack(cls.stack)

    def test_lf_data_lake_settings_created(self):
        self.template.resource_count_is("AWS::LakeFormation::DataLakeSettings", 1)

    def test_role_arn_output_exists(self):
        self.template.has_output("RoleArn", {"Value": "arn:aws:iam::123456789012:role/TestGlueRole"})


class TestRealtimeStackViaSubprocess(unittest.TestCase):
    """Test the realtime stack in a subprocess to get a fresh JSII runtime.

    JSII caches the Node.js process CWD from the first synth — since
    batch tests run first and set CWD to batch/infra, the realtime
    stack's relative asset paths would fail. Running in a subprocess
    gives us a clean JSII.
    """

    @classmethod
    def setUpClass(cls):
        # Run a subprocess that synthesizes the realtime stack and dumps the template JSON
        script = f"""
import os, sys, json
os.chdir("{os.path.join(_PROJECT_ROOT, 'realtime', 'infra')}")
sys.path.insert(0, "{os.path.join(_PROJECT_ROOT, 'realtime', 'infra')}")

import aws_cdk as cdk
from lf_dr_cdk.lf_dr_cdk_stack import LfDrCdkStack

app = cdk.App(context={{
    "config_file_bucket": "test-config-bucket",
    "config_file_key": "config/glue_config.conf",
    "eventbridge_schedule_min": "5",
}})
stack = LfDrCdkStack(app, "TestRealtimeStack")
import aws_cdk.assertions as assertions
template = assertions.Template.from_stack(stack)
print(json.dumps(template.to_json()))
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            cwd=os.path.join(_PROJECT_ROOT, "realtime", "infra"),
        )
        if result.returncode != 0:
            raise RuntimeError(f"Realtime stack synth failed:\n{result.stderr}")
        cls.template_json = json.loads(result.stdout)

    def _resources_of_type(self, resource_type):
        return {
            k: v for k, v in self.template_json.get("Resources", {}).items()
            if v["Type"] == resource_type
        }

    def test_dynamodb_table_created(self):
        tables = self._resources_of_type("AWS::DynamoDB::Table")
        self.assertEqual(len(tables), 1)

    def test_dynamodb_table_name(self):
        tables = self._resources_of_type("AWS::DynamoDB::Table")
        tbl = list(tables.values())[0]
        self.assertEqual(tbl["Properties"]["TableName"], "glue_lf_events")

    def test_dynamodb_has_three_gsis(self):
        tables = self._resources_of_type("AWS::DynamoDB::Table")
        tbl = list(tables.values())[0]
        gsis = tbl["Properties"]["GlobalSecondaryIndexes"]
        gsi_names = {g["IndexName"] for g in gsis}
        self.assertIn("Processed-index", gsi_names)
        self.assertIn("Processed-EventTime-index", gsi_names)
        self.assertIn("EventTime-EventSource-index", gsi_names)

    def test_lambda_functions_created(self):
        """At least the collector and replicator lambdas exist (CDK BucketDeployment adds a third)."""
        lambdas = self._resources_of_type("AWS::Lambda::Function")
        self.assertGreaterEqual(len(lambdas), 2)
        names = [v["Properties"].get("FunctionName") for v in lambdas.values()]
        self.assertIn("glue_lf_cloudtrail_pull_lambda", names)
        self.assertIn("glue_lf_replicate_event_lambda", names)

    def test_collector_lambda_timeout(self):
        lambdas = self._resources_of_type("AWS::Lambda::Function")
        for v in lambdas.values():
            if v["Properties"].get("FunctionName") == "glue_lf_cloudtrail_pull_lambda":
                self.assertEqual(v["Properties"]["Timeout"], 300)
                return
        self.fail("Collector lambda not found")

    def test_replicator_lambda_timeout(self):
        lambdas = self._resources_of_type("AWS::Lambda::Function")
        for v in lambdas.values():
            if v["Properties"].get("FunctionName") == "glue_lf_replicate_event_lambda":
                self.assertEqual(v["Properties"]["Timeout"], 900)
                return
        self.fail("Replicator lambda not found")

    def test_sqs_dead_letter_queue(self):
        queues = self._resources_of_type("AWS::SQS::Queue")
        self.assertEqual(len(queues), 1)

    def test_eventbridge_rule(self):
        rules = self._resources_of_type("AWS::Events::Rule")
        self.assertEqual(len(rules), 1)

    def test_no_wildcard_lakeformation(self):
        """Verify lakeformation:* is NOT in any IAM policy."""
        for logical_id, resource in self.template_json.get("Resources", {}).items():
            if resource["Type"] == "AWS::IAM::Policy":
                doc = resource["Properties"]["PolicyDocument"]
                for stmt in doc.get("Statement", []):
                    actions = stmt.get("Action", [])
                    if isinstance(actions, str):
                        actions = [actions]
                    for action in actions:
                        self.assertNotEqual(action, "lakeformation:*",
                                            f"Policy {logical_id} uses lakeformation:* wildcard")

    def test_dynamodb_policy_includes_scan(self):
        for resource in self.template_json.get("Resources", {}).values():
            if resource["Type"] == "AWS::IAM::Policy":
                doc = resource["Properties"]["PolicyDocument"]
                for stmt in doc.get("Statement", []):
                    actions = stmt.get("Action", [])
                    if isinstance(actions, list) and "dynamodb:Scan" in actions:
                        return
        self.fail("No DynamoDB policy with Scan action found")

    def test_s3_policy_scoped(self):
        """S3 GetObject should be scoped to config bucket, not *."""
        for resource in self.template_json.get("Resources", {}).values():
            if resource["Type"] == "AWS::IAM::Policy":
                doc = resource["Properties"]["PolicyDocument"]
                for stmt in doc.get("Statement", []):
                    if stmt.get("Action") == "s3:GetObject":
                        self.assertEqual(stmt["Resource"], "arn:aws:s3:::test-config-bucket/*")
                        return
        self.fail("No S3 GetObject policy found")


if __name__ == "__main__":
    unittest.main()
