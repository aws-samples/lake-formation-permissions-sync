"""Tests for the cloudtrail_to_boto3 converter.

Verifies that the botocore service-model-based converter correctly
transforms CloudTrail requestParameters into boto3 API format.
"""

import importlib.util
import os
import sys
import unittest

_REPLICATOR_DIR = os.path.join(os.path.dirname(__file__), "..", "realtime", "event_replicator")
sys.path.insert(0, _REPLICATOR_DIR)

from cloudtrail_to_boto3 import NAME_MAP, cloudtrail_to_boto3_converter  # noqa: E402

# Load replicator's lambda_function via importlib to avoid collision with other lambda_function modules
_spec = importlib.util.spec_from_file_location(
    "replicator_lambda_ct",
    os.path.join(_REPLICATOR_DIR, "lambda_function.py"),
)
_replicator_lf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_replicator_lf)


class TestNameMapBuilt(unittest.TestCase):
    """Verify that the service model name map was built correctly."""

    def test_name_map_is_populated(self):
        self.assertGreater(len(NAME_MAP), 900, "Expected 900+ mappings from Glue + LakeFormation service models")

    def test_known_glue_params(self):
        """Spot-check key Glue parameter names."""
        expected = {
            "databasename": "DatabaseName",
            "tableinput": "TableInput",
            "storagedescriptor": "StorageDescriptor",
            "catalogid": "CatalogId",
            "partitionkeys": "PartitionKeys",
            "numberofbuckets": "NumberOfBuckets",
        }
        for lower, pascal in expected.items():
            self.assertEqual(NAME_MAP.get(lower), pascal, f"Expected NAME_MAP['{lower}'] == '{pascal}'")

    def test_known_lakeformation_params(self):
        """Spot-check key Lake Formation parameter names."""
        expected = {
            "datalakesettings": "DataLakeSettings",
            "datalakeadmins": "DataLakeAdmins",
            "permissions": "Permissions",
            "principal": "Principal",
            "resource": "Resource",
            "lftags": "LFTags",
            "tagkey": "TagKey",
            "tagvalues": "TagValues",
        }
        for lower, pascal in expected.items():
            self.assertEqual(NAME_MAP.get(lower), pascal, f"Expected NAME_MAP['{lower}'] == '{pascal}'")


class TestCloudtrailToBoto3Converter(unittest.TestCase):
    """Test the recursive converter function."""

    def test_flat_dict(self):
        """Simple flat dictionary conversion."""
        input_data = {"databasename": "my_db", "catalogid": "123456789"}
        result = cloudtrail_to_boto3_converter(input_data)
        self.assertEqual(result, {"DatabaseName": "my_db", "CatalogId": "123456789"})

    def test_nested_dict(self):
        """Nested dictionary conversion."""
        input_data = {
            "databaseinput": {
                "name": "test_db",
                "description": "A test database",
                "locationuri": "s3://my-bucket/path",
            }
        }
        result = cloudtrail_to_boto3_converter(input_data)
        self.assertEqual(result["DatabaseInput"]["Name"], "test_db")
        self.assertEqual(result["DatabaseInput"]["Description"], "A test database")
        self.assertEqual(result["DatabaseInput"]["LocationUri"], "s3://my-bucket/path")

    def test_list_of_dicts(self):
        """List containing dictionaries."""
        input_data = {
            "entries": [
                {"principal": {"datalakeprincipalidentifier": "arn:aws:iam::123:role/test"}},
                {"permissions": ["ALL"]},
            ]
        }
        result = cloudtrail_to_boto3_converter(input_data)
        self.assertEqual(result["Entries"][0]["Principal"]["DataLakePrincipalIdentifier"], "arn:aws:iam::123:role/test")

    def test_scalar_values_unchanged(self):
        """Scalar values should pass through untouched."""
        input_data = {"databasename": "my_db", "catalogid": 12345}
        result = cloudtrail_to_boto3_converter(input_data)
        self.assertEqual(result["CatalogId"], 12345)

    def test_string_list_values_not_transformed(self):
        """String values inside lists should NOT be treated as keys.

        This was a bug in the original implementation where parse_list()
        ran string values through key_alias, potentially corrupting data.
        """
        input_data = {
            "permissions": ["ALL", "SELECT", "INSERT"],
            "tagvalues": ["production", "development"],
        }
        result = cloudtrail_to_boto3_converter(input_data)
        # Values should remain exactly as they were
        self.assertEqual(result["Permissions"], ["ALL", "SELECT", "INSERT"])
        self.assertEqual(result["TagValues"], ["production", "development"])

    def test_unknown_keys_pass_through(self):
        """Keys not in the service model should pass through unchanged."""
        input_data = {"someFutureParam": "value", "databasename": "db"}
        result = cloudtrail_to_boto3_converter(input_data)
        self.assertIn("someFutureParam", result)
        self.assertEqual(result["someFutureParam"], "value")

    def test_empty_inputs(self):
        """Empty structures should return empty structures."""
        self.assertEqual(cloudtrail_to_boto3_converter({}), {})
        self.assertEqual(cloudtrail_to_boto3_converter([]), [])

    def test_deeply_nested_create_table(self):
        """Realistic CreateTable event parameters."""
        input_data = {
            "databasename": "analytics",
            "tableinput": {
                "name": "users",
                "storagedescriptor": {
                    "columns": [
                        {"name": "id", "type": "string"},
                        {"name": "email", "type": "string"},
                    ],
                    "location": "s3://data-bucket/analytics/users/",
                    "inputformat": "org.apache.hadoop.mapred.TextInputFormat",
                    "outputformat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "serdeinfo": {
                        "serializationlibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    },
                    "numberofbuckets": "-1",
                    "compressed": False,
                    "storedassubdirectories": False,
                },
                "partitionkeys": [
                    {"name": "year", "type": "string"},
                ],
                "tabletype": "EXTERNAL_TABLE",
                "retention": "0",
            },
        }
        result = cloudtrail_to_boto3_converter(input_data)

        self.assertEqual(result["DatabaseName"], "analytics")
        self.assertEqual(result["TableInput"]["Name"], "users")
        self.assertEqual(result["TableInput"]["StorageDescriptor"]["NumberOfBuckets"], "-1")
        self.assertEqual(result["TableInput"]["StorageDescriptor"]["Columns"][0]["Name"], "id")
        self.assertEqual(
            result["TableInput"]["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"],
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        )
        self.assertEqual(result["TableInput"]["PartitionKeys"][0]["Name"], "year")


class TestGetS3TableTargetBucketName(unittest.TestCase):
    """Test the S3 bucket extraction helper."""

    def test_standard_path(self):
        _get_s3_table_target_bucket_name = _replicator_lf._get_s3_table_target_bucket_name

        self.assertEqual(
            _get_s3_table_target_bucket_name("s3://my-bucket/path/to/data"),
            "my-bucket",
        )

    def test_trailing_slash(self):
        _get_s3_table_target_bucket_name = _replicator_lf._get_s3_table_target_bucket_name

        self.assertEqual(
            _get_s3_table_target_bucket_name("s3://my-bucket-name/path/to/data/"),
            "my-bucket-name",
        )

    def test_dotted_bucket(self):
        _get_s3_table_target_bucket_name = _replicator_lf._get_s3_table_target_bucket_name

        self.assertEqual(
            _get_s3_table_target_bucket_name("s3://my.bucket.name.with.dots/path/to/data"),
            "my.bucket.name.with.dots",
        )


if __name__ == "__main__":
    unittest.main()
