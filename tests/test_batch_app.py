"""Tests for the batch script app.py.

Covers:
- update_location (S3 URL remapping via urlparse)
- update_table_location (Hive, Iceberg metadata_location, previous_metadata_location, AdditionalLocations)
- update_database_location
- rewrite_iceberg_metadata (S3 metadata file rewriting + .avro manifest rewriting)
- _rewrite_avro_file / _rewrite_avro_record (Avro binary rewriting)
- _process_restore_line ordering (database, table, partition routing)
"""

import io
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

    @patch.object(app, "get_client")
    def test_rewrites_avro_manifest_files(self, mock_get_client):
        """Verify that .avro manifest-list files referenced in metadata JSON are rewritten."""
        import fastavro

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        # --- Build a fake Avro manifest-list file with source bucket paths ---
        manifest_list_schema = {
            "type": "record",
            "name": "manifest_file",
            "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
            ],
        }
        manifest_list_records = [
            {
                "manifest_path": "s3://src-bucket-east/warehouse/db/tbl/metadata/m0.avro",
                "manifest_length": 1234,
                "partition_spec_id": 0,
            },
        ]
        avro_buf = io.BytesIO()
        fastavro.writer(avro_buf, manifest_list_schema, manifest_list_records)
        avro_bytes = avro_buf.getvalue()

        # --- Build a fake Avro manifest file (the child m0.avro) ---
        manifest_schema = {
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {"name": "status", "type": "int"},
                {
                    "name": "data_file",
                    "type": {
                        "type": "record",
                        "name": "data_file",
                        "fields": [
                            {"name": "file_path", "type": "string"},
                            {"name": "file_size_in_bytes", "type": "long"},
                        ],
                    },
                },
            ],
        }
        manifest_records = [
            {
                "status": 1,
                "data_file": {
                    "file_path": "s3://src-bucket-east/warehouse/db/tbl/data/part-00000.parquet",
                    "file_size_in_bytes": 5678,
                },
            },
        ]
        child_avro_buf = io.BytesIO()
        fastavro.writer(child_avro_buf, manifest_schema, manifest_records)
        child_avro_bytes = child_avro_buf.getvalue()

        # --- JSON metadata referencing the manifest-list .avro ---
        metadata_json = json.dumps(
            {
                "format-version": 2,
                "location": "s3://src-bucket-east/warehouse/db/tbl",
                "snapshots": [
                    {
                        "snapshot-id": 100,
                        "manifest-list": "s3://src-bucket-east/warehouse/db/tbl/metadata/snap-100.avro",
                    }
                ],
            }
        )

        # --- Mock S3 responses: metadata JSON, manifest-list .avro, manifest .avro ---
        def mock_get_object(Bucket, Key):
            if Key.endswith(".metadata.json"):
                return {"Body": MagicMock(read=MagicMock(return_value=metadata_json.encode("utf-8")))}
            elif "snap-100.avro" in Key:
                return {"Body": MagicMock(read=MagicMock(return_value=avro_bytes))}
            elif "m0.avro" in Key:
                return {"Body": MagicMock(read=MagicMock(return_value=child_avro_bytes))}
            raise ValueError(f"Unexpected key: {Key}")

        mock_s3.get_object.side_effect = mock_get_object

        # --- Invoke ---
        result = app.rewrite_iceberg_metadata(
            "s3://src-bucket-east/warehouse/db/tbl/metadata/v2.metadata.json",
            self.mapping,
            "us-east-1",
        )

        self.assertEqual(result, "s3://tgt-bucket-west/warehouse/db/tbl/metadata/v2.metadata.json")

        # Verify put_object was called 3 times: JSON + manifest-list .avro + manifest .avro
        self.assertEqual(mock_s3.put_object.call_count, 3)

        # Check the manifest-list .avro was rewritten
        avro_puts = [
            c for c in mock_s3.put_object.call_args_list if c[1].get("ContentType") == "application/avro"
        ]
        self.assertEqual(len(avro_puts), 2)

        # Parse the rewritten manifest-list to verify paths are remapped
        snap_put = [c for c in avro_puts if "snap-100" in c[1]["Key"]][0]
        rewritten_records = list(fastavro.reader(io.BytesIO(snap_put[1]["Body"])))
        self.assertEqual(
            rewritten_records[0]["manifest_path"],
            "s3://tgt-bucket-west/warehouse/db/tbl/metadata/m0.avro",
        )

        # Parse the rewritten manifest to verify data file paths are remapped
        m0_put = [c for c in avro_puts if "m0.avro" in c[1]["Key"]][0]
        rewritten_manifest = list(fastavro.reader(io.BytesIO(m0_put[1]["Body"])))
        self.assertEqual(
            rewritten_manifest[0]["data_file"]["file_path"],
            "s3://tgt-bucket-west/warehouse/db/tbl/data/part-00000.parquet",
        )


class TestRewriteAvroRecord(unittest.TestCase):
    """Test the _rewrite_avro_record and _collect_avro_uris helpers."""

    def test_rewrites_nested_strings(self):
        mapping = {"src-bucket": "tgt-bucket"}
        record = {
            "manifest_path": "s3://src-bucket/meta/m0.avro",
            "data_file": {
                "file_path": "s3://src-bucket/data/part-0.parquet",
                "size": 100,
            },
            "tags": ["s3://src-bucket/tags/a.txt", "other"],
        }
        app._rewrite_avro_record(record, mapping)
        self.assertEqual(record["manifest_path"], "s3://tgt-bucket/meta/m0.avro")
        self.assertEqual(record["data_file"]["file_path"], "s3://tgt-bucket/data/part-0.parquet")
        self.assertEqual(record["tags"][0], "s3://tgt-bucket/tags/a.txt")
        self.assertEqual(record["tags"][1], "other")

    def test_leaves_non_matching_strings(self):
        mapping = {"src-bucket": "tgt-bucket"}
        record = {"path": "s3://other-bucket/data/file.parquet"}
        app._rewrite_avro_record(record, mapping)
        self.assertEqual(record["path"], "s3://other-bucket/data/file.parquet")

    def test_collect_avro_uris_finds_source_uris(self):
        mapping = {"src-bucket": "tgt-bucket"}
        record = {
            "manifest_path": "s3://src-bucket/meta/m0.avro",
            "data_file": {"file_path": "s3://src-bucket/data/part-0.parquet"},
        }
        uris: list[str] = []
        app._collect_avro_uris(record, mapping, uris)
        # Only .avro files are collected, not .parquet
        self.assertEqual(uris, ["s3://src-bucket/meta/m0.avro"])

    def test_collect_avro_uris_ignores_unmapped_buckets(self):
        mapping = {"src-bucket": "tgt-bucket"}
        record = {"manifest_path": "s3://other-bucket/meta/m0.avro"}
        uris: list[str] = []
        app._collect_avro_uris(record, mapping, uris)
        self.assertEqual(uris, [])


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
