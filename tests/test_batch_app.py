"""Tests for the batch script app.py.

Covers:
- update_location (S3 URL remapping via urlparse)
- update_table_location (Hive, Iceberg metadata_location, previous_metadata_location, AdditionalLocations)
- update_database_location
- rewrite_iceberg_metadata (S3 metadata file rewriting)
- _process_restore_line ordering (database, table, partition routing)
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the batch source to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "batch", "script"))

# Mock awswrangler and awsglue before importing app
sys.modules["awswrangler"] = MagicMock()
sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()

import app  # noqa: E402


class TestUpdateLocation(unittest.TestCase):
    """Test the S3 URL remapping function."""

    def setUp(self):
        self.mapping = {
            "src-bucket-east": "tgt-bucket-west",
            "lf-metadata-111-us-east-1": "lf-metadata-111-us-west-2",
        }

    def test_simple_remap(self):
        result = app.update_location("s3://src-bucket-east/db/table/data/", self.mapping)
        self.assertEqual(result, "s3://tgt-bucket-west/db/table/data/")

    def test_no_match_passthrough(self):
        result = app.update_location("s3://other-bucket/path/", self.mapping)
        self.assertEqual(result, "s3://other-bucket/path/")

    def test_empty_string(self):
        result = app.update_location("", self.mapping)
        self.assertEqual(result, "")

    def test_none_returns_none(self):
        result = app.update_location(None, self.mapping)
        self.assertIsNone(result)

    def test_preserves_full_path(self):
        result = app.update_location(
            "s3://lf-metadata-111-us-east-1/warehouse/db/table/metadata/v5.metadata.json",
            self.mapping,
        )
        self.assertEqual(
            result,
            "s3://lf-metadata-111-us-west-2/warehouse/db/table/metadata/v5.metadata.json",
        )

    def test_uses_urlparse_correctly(self):
        """Verify it uses netloc matching not string replace (avoids partial bucket name matches)."""
        mapping = {"bucket": "replaced"}
        # 'my-bucket' should NOT match 'bucket' because urlparse gives netloc='my-bucket'
        result = app.update_location("s3://my-bucket/path/", mapping)
        self.assertEqual(result, "s3://my-bucket/path/")


class TestUpdateTableLocation(unittest.TestCase):
    """Test all S3 location remapping in table definitions."""

    def setUp(self):
        self.mapping = {
            "src-bucket": "tgt-bucket",
            "meta-east": "meta-west",
        }

    def test_remaps_storage_descriptor_location(self):
        table = {
            "StorageDescriptor": {"Location": "s3://src-bucket/db/tbl/"},
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(result["StorageDescriptor"]["Location"], "s3://tgt-bucket/db/tbl/")

    def test_remaps_metadata_location(self):
        table = {
            "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
            "Parameters": {
                "metadata_location": "s3://meta-east/db/tbl/metadata/v3.metadata.json",
            },
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(
            result["Parameters"]["metadata_location"],
            "s3://meta-west/db/tbl/metadata/v3.metadata.json",
        )

    def test_remaps_previous_metadata_location(self):
        table = {
            "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
            "Parameters": {
                "metadata_location": "s3://meta-east/v3.metadata.json",
                "previous_metadata_location": "s3://meta-east/v2.metadata.json",
            },
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(
            result["Parameters"]["previous_metadata_location"],
            "s3://meta-west/v2.metadata.json",
        )

    def test_remaps_additional_locations(self):
        table = {
            "StorageDescriptor": {
                "Location": "s3://src-bucket/data/",
                "AdditionalLocations": [
                    "s3://src-bucket/extra1/",
                    "s3://src-bucket/extra2/",
                ],
            },
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(
            result["StorageDescriptor"]["AdditionalLocations"],
            ["s3://tgt-bucket/extra1/", "s3://tgt-bucket/extra2/"],
        )

    def test_no_parameters_key_ok(self):
        """Tables without Parameters should not crash."""
        table = {
            "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(result["StorageDescriptor"]["Location"], "s3://tgt-bucket/data/")

    def test_empty_additional_locations_noop(self):
        table = {
            "StorageDescriptor": {
                "Location": "s3://src-bucket/data/",
                "AdditionalLocations": [],
            },
        }
        result = app.update_table_location(table, self.mapping)
        self.assertEqual(result["StorageDescriptor"]["AdditionalLocations"], [])


class TestUpdateDatabaseLocation(unittest.TestCase):
    """Test database LocationUri remapping."""

    def test_remaps_location_uri(self):
        db = {"Name": "mydb", "LocationUri": "s3://src-bucket/mydb/"}
        mapping = {"src-bucket": "tgt-bucket"}
        result = app.update_database_location(db, mapping)
        self.assertEqual(result["LocationUri"], "s3://tgt-bucket/mydb/")

    def test_no_location_uri(self):
        db = {"Name": "mydb", "Description": "test"}
        mapping = {"src-bucket": "tgt-bucket"}
        result = app.update_database_location(db, mapping)
        self.assertNotIn("LocationUri", result)


class TestRewriteIcebergMetadata(unittest.TestCase):
    """Test the Iceberg metadata JSON rewriting."""

    def setUp(self):
        self.mapping = {
            "src-bucket-east": "tgt-bucket-west",
            "data-east": "data-west",
        }

    @patch.object(app, "get_client")
    def test_rewrites_s3_uris(self, mock_get_client):
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        metadata_json = json.dumps(
            {
                "format-version": 2,
                "location": "s3://src-bucket-east/warehouse/db/tbl",
                "current-snapshot-id": 123,
                "snapshots": [
                    {
                        "snapshot-id": 123,
                        "manifest-list": "s3://src-bucket-east/warehouse/db/tbl/metadata/snap-123.avro",
                    }
                ],
                "metadata-log": [
                    {
                        "metadata-file": "s3://src-bucket-east/warehouse/db/tbl/metadata/v1.metadata.json",
                    }
                ],
            }
        )
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=metadata_json.encode("utf-8")))
        }

        result = app.rewrite_iceberg_metadata(
            "s3://src-bucket-east/warehouse/db/tbl/metadata/v2.metadata.json",
            self.mapping,
            "us-east-1",
        )

        self.assertEqual(result, "s3://tgt-bucket-west/warehouse/db/tbl/metadata/v2.metadata.json")

        # Verify the put_object was called with rewritten content
        put_call = mock_s3.put_object.call_args
        written_body = (
            put_call[1]["Body"].decode("utf-8") if isinstance(put_call[1]["Body"], bytes) else put_call[1]["Body"]
        )
        written_data = json.loads(written_body)
        self.assertEqual(written_data["location"], "s3://tgt-bucket-west/warehouse/db/tbl")
        self.assertIn("tgt-bucket-west", written_data["snapshots"][0]["manifest-list"])

    @patch.object(app, "get_client")
    def test_rewrites_s3a_uris(self, mock_get_client):
        """Spark/Trino use s3a:// protocol — verify those are rewritten too."""
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        metadata_json = json.dumps(
            {
                "location": "s3a://src-bucket-east/warehouse/db/tbl",
            }
        )
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=metadata_json.encode("utf-8")))
        }

        app.rewrite_iceberg_metadata(
            "s3://src-bucket-east/metadata/v1.metadata.json",
            self.mapping,
            "us-east-1",
        )

        put_call = mock_s3.put_object.call_args
        written_body = put_call[1]["Body"].decode("utf-8")
        written_data = json.loads(written_body)
        self.assertEqual(written_data["location"], "s3a://tgt-bucket-west/warehouse/db/tbl")

    def test_unmapped_bucket_skips_rewrite(self):
        result = app.rewrite_iceberg_metadata(
            "s3://unknown-bucket/metadata/v1.metadata.json",
            self.mapping,
            "us-east-1",
        )
        self.assertEqual(result, "s3://unknown-bucket/metadata/v1.metadata.json")

    def test_empty_uri_passthrough(self):
        result = app.rewrite_iceberg_metadata("", self.mapping, "us-east-1")
        self.assertEqual(result, "")

    def test_none_uri_passthrough(self):
        result = app.rewrite_iceberg_metadata(None, self.mapping, "us-east-1")
        self.assertIsNone(result)


class TestProcessRestoreLine(unittest.TestCase):
    """Test the restore line processor routes correctly."""

    def test_database_line(self):
        db_data = json.dumps({"Name": "mydb", "Description": "test"})
        line = f"database\tmydb\t\t{db_data}"
        mock_glue = MagicMock()

        otype, db_name = app._process_restore_line(mock_glue, line, False, {})
        self.assertEqual(otype, "database")
        self.assertEqual(db_name, "mydb")

    def test_table_line(self):
        table_data = json.dumps(
            {
                "Name": "mytable",
                "StorageDescriptor": {"Location": "s3://bucket/data/"},
            }
        )
        line = f"table\tmydb\tmytable\t{table_data}"
        mock_glue = MagicMock()

        otype, db_name = app._process_restore_line(mock_glue, line, False, {})
        self.assertEqual(otype, "table")
        self.assertEqual(db_name, "mydb")

    def test_partition_line(self):
        partition_data = json.dumps(
            {
                "Values": ["2025"],
                "StorageDescriptor": {"Location": "s3://bucket/data/year=2025/"},
                "Parameters": {},
            }
        )
        line = f"partition\tmydb\tmytable\t{partition_data}"
        mock_glue = MagicMock()

        otype, db_name = app._process_restore_line(mock_glue, line, False, {})
        self.assertEqual(otype, "partition")
        self.assertEqual(db_name, "mydb")

    @patch.object(app, "rewrite_iceberg_metadata")
    def test_iceberg_table_triggers_metadata_rewrite(self, mock_rewrite):
        table_data = json.dumps(
            {
                "Name": "iceberg_tbl",
                "StorageDescriptor": {"Location": "s3://bucket/data/"},
                "Parameters": {
                    "metadata_location": "s3://bucket/metadata/v1.metadata.json",
                    "table_type": "ICEBERG",
                },
            }
        )
        line = f"table\tmydb\ticeberg_tbl\t{table_data}"
        mock_glue = MagicMock()
        mapping = {"bucket": "target-bucket"}

        app._process_restore_line(mock_glue, line, True, mapping, "us-east-1")
        mock_rewrite.assert_called_once()

    def test_non_iceberg_table_no_metadata_rewrite(self):
        table_data = json.dumps(
            {
                "Name": "hive_tbl",
                "StorageDescriptor": {"Location": "s3://bucket/data/"},
            }
        )
        line = f"table\tmydb\thive_tbl\t{table_data}"
        mock_glue = MagicMock()

        with patch.object(app, "rewrite_iceberg_metadata") as mock_rewrite:
            app._process_restore_line(mock_glue, line, True, {"bucket": "tgt"}, "us-east-1")
            mock_rewrite.assert_not_called()


if __name__ == "__main__":
    unittest.main()
