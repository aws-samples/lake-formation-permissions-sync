"""Tests for the realtime replicate-event Lambda function.

Covers all the new features added:
- Omitted requestParameters detection and source-fetch fallback
- S3 location remapping (Hive, Iceberg metadata_location, previous_metadata_location, AdditionalLocations)
- Partition location remapping (CreatePartition, BatchCreatePartition)
- Checkpoint with high-water-mark (out-of-order event handling)
- Dispatch table completeness
"""

import importlib.util
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

_REPLICATOR_DIR = os.path.join(os.path.dirname(__file__), "..", "realtime", "event_replicator")
sys.path.insert(0, _REPLICATOR_DIR)

# Use importlib to avoid collision with other lambda_function modules on sys.path
_spec = importlib.util.spec_from_file_location(
    "replicator_lambda",
    os.path.join(_REPLICATOR_DIR, "lambda_function.py"),
)
lf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lf)


class TestIsOmittedEvent(unittest.TestCase):
    """Test detection of CloudTrail omitted requestParameters."""

    def test_omitted_true(self):
        event = {"reason": "requestParameters too large", "omitted": "true", "originalSize": "149893"}
        self.assertTrue(lf._is_omitted_event(event))

    def test_omitted_only_flag(self):
        event = {"omitted": "true"}
        self.assertTrue(lf._is_omitted_event(event))

    def test_omitted_only_reason(self):
        event = {"reason": "requestParameters too large"}
        self.assertTrue(lf._is_omitted_event(event))

    def test_normal_event_not_omitted(self):
        event = {"DatabaseName": "mydb", "TableInput": {"Name": "mytable"}}
        self.assertFalse(lf._is_omitted_event(event))

    def test_empty_event_not_omitted(self):
        self.assertFalse(lf._is_omitted_event({}))


class TestRemapSingleLocation(unittest.TestCase):
    """Test the _remap_single_location helper."""

    def setUp(self):
        # Ensure mapping is loaded
        lf._table_s3_mapping = {
            "source-bucket-east": "target-bucket-west",
            "lf-metadata-111-us-east-1": "lf-metadata-111-us-west-2",
        }

    def tearDown(self):
        lf._table_s3_mapping = None

    def test_remap_matching_bucket(self):
        result = lf._remap_single_location("s3://source-bucket-east/db/table/data/")
        self.assertEqual(result, "s3://target-bucket-west/db/table/data/")

    def test_remap_no_match_passthrough(self):
        result = lf._remap_single_location("s3://unrelated-bucket/path/")
        self.assertEqual(result, "s3://unrelated-bucket/path/")

    def test_remap_empty_string(self):
        result = lf._remap_single_location("")
        self.assertEqual(result, "")

    def test_remap_none(self):
        result = lf._remap_single_location(None)
        self.assertIsNone(result)

    def test_remap_iceberg_metadata_path(self):
        result = lf._remap_single_location(
            "s3://lf-metadata-111-us-east-1/warehouse/db/table/metadata/v1.metadata.json"
        )
        self.assertEqual(result, "s3://lf-metadata-111-us-west-2/warehouse/db/table/metadata/v1.metadata.json")


class TestRemapS3Location(unittest.TestCase):
    """Test the full _remap_s3_location function (table-level remapping)."""

    def setUp(self):
        lf._table_s3_mapping = {
            "src-bucket": "tgt-bucket",
            "meta-east": "meta-west",
        }
        lf._config = MagicMock()  # prevent lazy-load attempts

    def tearDown(self):
        lf._table_s3_mapping = None
        lf._config = None

    def test_remaps_storage_descriptor_location(self):
        params = {
            "TableInput": {
                "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
            }
        }
        lf._remap_s3_location(params)
        self.assertEqual(params["TableInput"]["StorageDescriptor"]["Location"], "s3://tgt-bucket/data/")

    def test_remaps_iceberg_metadata_location(self):
        params = {
            "TableInput": {
                "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
                "Parameters": {
                    "metadata_location": "s3://meta-east/db/tbl/metadata/v3.metadata.json",
                },
            }
        }
        lf._remap_s3_location(params)
        self.assertEqual(
            params["TableInput"]["Parameters"]["metadata_location"], "s3://meta-west/db/tbl/metadata/v3.metadata.json"
        )

    def test_remaps_previous_metadata_location(self):
        params = {
            "TableInput": {
                "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
                "Parameters": {
                    "metadata_location": "s3://meta-east/v3.metadata.json",
                    "previous_metadata_location": "s3://meta-east/v2.metadata.json",
                },
            }
        }
        lf._remap_s3_location(params)
        self.assertEqual(
            params["TableInput"]["Parameters"]["previous_metadata_location"], "s3://meta-west/v2.metadata.json"
        )

    def test_remaps_additional_locations(self):
        params = {
            "TableInput": {
                "StorageDescriptor": {
                    "Location": "s3://src-bucket/data/",
                    "AdditionalLocations": [
                        "s3://src-bucket/extra1/",
                        "s3://src-bucket/extra2/",
                    ],
                },
            }
        }
        lf._remap_s3_location(params)
        self.assertEqual(
            params["TableInput"]["StorageDescriptor"]["AdditionalLocations"],
            ["s3://tgt-bucket/extra1/", "s3://tgt-bucket/extra2/"],
        )

    def test_no_table_input_noop(self):
        params = {"DatabaseName": "mydb"}
        lf._remap_s3_location(params)
        self.assertEqual(params, {"DatabaseName": "mydb"})


class TestRemapPartitionLocation(unittest.TestCase):
    """Test partition S3 location remapping."""

    def setUp(self):
        lf._table_s3_mapping = {"src-bucket": "tgt-bucket"}
        lf._config = MagicMock()

    def tearDown(self):
        lf._table_s3_mapping = None
        lf._config = None

    def test_remaps_partition_location(self):
        sd = {"Location": "s3://src-bucket/db/tbl/part=1/"}
        lf._remap_partition_location(sd)
        self.assertEqual(sd["Location"], "s3://tgt-bucket/db/tbl/part=1/")

    def test_remaps_partition_additional_locations(self):
        sd = {"Location": "s3://src-bucket/db/tbl/part=1/", "AdditionalLocations": ["s3://src-bucket/extra/part=1/"]}
        lf._remap_partition_location(sd)
        self.assertEqual(sd["AdditionalLocations"], ["s3://tgt-bucket/extra/part=1/"])

    def test_empty_sd_noop(self):
        sd = {}
        lf._remap_partition_location(sd)
        self.assertEqual(sd, {})


class TestPreprocessCreatePartition(unittest.TestCase):
    """Test the CreatePartition preprocessor."""

    def setUp(self):
        lf._table_s3_mapping = {"src-bucket": "tgt-bucket"}
        lf._config = MagicMock()

    def tearDown(self):
        lf._table_s3_mapping = None
        lf._config = None

    def test_fixes_number_of_buckets_type(self):
        params = {
            "PartitionInput": {
                "StorageDescriptor": {
                    "NumberOfBuckets": "5",
                    "Location": "s3://src-bucket/data/",
                }
            }
        }
        result = lf.preprocess_create_partition(params)
        self.assertEqual(result["PartitionInput"]["StorageDescriptor"]["NumberOfBuckets"], 5)
        self.assertIsInstance(result["PartitionInput"]["StorageDescriptor"]["NumberOfBuckets"], int)

    def test_remaps_partition_location(self):
        params = {
            "PartitionInput": {
                "StorageDescriptor": {
                    "Location": "s3://src-bucket/db/tbl/year=2025/",
                }
            }
        }
        result = lf.preprocess_create_partition(params)
        self.assertEqual(result["PartitionInput"]["StorageDescriptor"]["Location"], "s3://tgt-bucket/db/tbl/year=2025/")


class TestPreprocessBatchCreatePartition(unittest.TestCase):
    """Test the BatchCreatePartition preprocessor."""

    def setUp(self):
        lf._table_s3_mapping = {"src-bucket": "tgt-bucket"}
        lf._config = MagicMock()

    def tearDown(self):
        lf._table_s3_mapping = None
        lf._config = None

    def test_remaps_all_partitions(self):
        params = {
            "PartitionInputList": [
                {"StorageDescriptor": {"Location": "s3://src-bucket/p1/", "NumberOfBuckets": "3"}},
                {"StorageDescriptor": {"Location": "s3://src-bucket/p2/", "NumberOfBuckets": "3"}},
            ]
        }
        result = lf.preprocess_batch_create_partition(params)
        self.assertEqual(result["PartitionInputList"][0]["StorageDescriptor"]["Location"], "s3://tgt-bucket/p1/")
        self.assertEqual(result["PartitionInputList"][1]["StorageDescriptor"]["Location"], "s3://tgt-bucket/p2/")
        self.assertEqual(result["PartitionInputList"][0]["StorageDescriptor"]["NumberOfBuckets"], 3)


class TestPreprocessTable(unittest.TestCase):
    """Test the table preprocessor (Create/UpdateTable)."""

    def setUp(self):
        lf._table_s3_mapping = {"src-bucket": "tgt-bucket"}
        lf._config = MagicMock()

    def tearDown(self):
        lf._table_s3_mapping = None
        lf._config = None

    def test_strips_row_filtering(self):
        params = {
            "TableInput": {
                "isRowFilteringEnabled": True,
                "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
                "Retention": "0",
            }
        }
        result = lf.preprocess_table(params)
        self.assertNotIn("isRowFilteringEnabled", result["TableInput"])

    def test_fixes_numeric_types(self):
        params = {
            "TableInput": {
                "StorageDescriptor": {"NumberOfBuckets": "10", "Location": ""},
                "Retention": "30",
            }
        }
        result = lf.preprocess_table(params)
        self.assertEqual(result["TableInput"]["StorageDescriptor"]["NumberOfBuckets"], 10)
        self.assertEqual(result["TableInput"]["Retention"], 30)


class TestFetchTableFromSource(unittest.TestCase):
    """Test the fallback fetch for omitted requestParameters."""

    def test_extracts_db_and_table_from_arn(self):
        full_event = {
            "resources": [
                {"ARN": "arn:aws:glue:us-east-1:123456:catalog", "type": "AWS::Glue::Catalog"},
                {"ARN": "arn:aws:glue:us-east-1:123456:database/cw_eafi", "type": "AWS::Glue::Database"},
                {"ARN": "arn:aws:glue:us-east-1:123456:table/cw_eafi/test_table", "type": "AWS::Glue::Table"},
            ]
        }
        mock_glue = MagicMock()
        mock_glue.get_table.return_value = {
            "Table": {
                "Name": "test_table",
                "DatabaseName": "cw_eafi",
                "CatalogId": "123456",
                "CreateTime": "2025-01-01",
                "StorageDescriptor": {"Location": "s3://bucket/data/"},
            }
        }

        with patch.object(lf, "_get_client", return_value=mock_glue):
            result = lf._fetch_table_from_source(full_event)

        self.assertIsNotNone(result)
        self.assertEqual(result["DatabaseName"], "cw_eafi")
        self.assertEqual(result["TableInput"]["Name"], "test_table")
        # Should have stripped non-input fields
        self.assertNotIn("DatabaseName", result["TableInput"])
        self.assertNotIn("CatalogId", result["TableInput"])
        self.assertNotIn("CreateTime", result["TableInput"])

    def test_returns_none_when_no_table_arn(self):
        full_event = {
            "resources": [
                {"ARN": "arn:aws:glue:us-east-1:123456:catalog"},
            ]
        }
        result = lf._fetch_table_from_source(full_event)
        self.assertIsNone(result)


class TestDispatchTableCompleteness(unittest.TestCase):
    """Verify the dispatch table has all expected events."""

    def test_create_partition_in_dispatch(self):
        self.assertIn("CreatePartition", lf.EVENT_DISPATCH)

    def test_batch_create_partition_in_dispatch(self):
        self.assertIn("BatchCreatePartition", lf.EVENT_DISPATCH)

    def test_all_glue_events_present(self):
        expected = [
            "CreateTable",
            "UpdateTable",
            "DeleteTable",
            "CreateDatabase",
            "UpdateDatabase",
            "DeleteDatabase",
            "BatchCreatePartition",
            "CreatePartition",
        ]
        for event in expected:
            self.assertIn(event, lf.EVENT_DISPATCH, f"Missing dispatch entry: {event}")

    def test_all_lf_events_present(self):
        expected = [
            "RegisterResource",
            "DeregisterResource",
            "PutDataLakeSettings",
            "CreateLFTag",
            "DeleteLFTag",
            "UpdateLFTag",
            "AddLFTagsToResource",
            "BatchGrantPermissions",
            "BatchRevokePermissions",
            "GrantPermissions",
            "RevokePermissions",
        ]
        for event in expected:
            self.assertIn(event, lf.EVENT_DISPATCH, f"Missing dispatch entry: {event}")

    def test_create_partition_uses_correct_preprocessor(self):
        client, method, preprocessor, errors = lf.EVENT_DISPATCH["CreatePartition"]
        self.assertEqual(client, "glue")
        self.assertEqual(method, "create_partition")
        self.assertEqual(preprocessor, lf.preprocess_create_partition)

    def test_create_table_uses_correct_preprocessor(self):
        client, method, preprocessor, errors = lf.EVENT_DISPATCH["CreateTable"]
        self.assertEqual(preprocessor, lf.preprocess_table)


class TestCheckpointHighWaterMark(unittest.TestCase):
    """Test the transactional checkpoint with out-of-order handling."""

    def setUp(self):
        lf._config = MagicMock()
        lf._config.__getitem__ = MagicMock(return_value={"source_region": "us-east-1"})

    def tearDown(self):
        lf._config = None
        lf._clients.clear()

    @patch.dict(os.environ, {"TABLE_NAME": "test_table"})
    def test_successful_checkpoint_advance(self):
        mock_ddb = MagicMock()
        lf._clients["ddb_client"] = mock_ddb

        result = lf.mark_event_and_update_checkpoint("event-123", "20251105152618")
        self.assertEqual(result, "Y")
        mock_ddb.transact_write_items.assert_called_once()

        # Verify transaction structure
        call_args = mock_ddb.transact_write_items.call_args
        items = call_args[1]["TransactItems"] if "TransactItems" in call_args[1] else call_args[0][0]
        self.assertEqual(len(items), 2)  # event update + checkpoint update

    @patch.dict(os.environ, {"TABLE_NAME": "test_table"})
    def test_out_of_order_event_still_marked_processed(self):
        """When checkpoint condition fails (out-of-order), event should still be marked."""
        from botocore.errorfactory import ClientError

        mock_ddb = MagicMock()
        lf._clients["ddb_client"] = mock_ddb

        # Simulate TransactionCanceledException where only checkpoint fails
        error_response = {
            "Error": {"Code": "TransactionCanceledException", "Message": "Transaction cancelled"},
            "CancellationReasons": [
                {"Code": "None"},  # event row succeeded
                {"Code": "ConditionalCheckFailed"},  # checkpoint condition failed
            ],
        }
        mock_ddb.transact_write_items.side_effect = ClientError(error_response, "TransactWriteItems")

        result = lf.mark_event_and_update_checkpoint("event-456", "20251105152600")
        self.assertEqual(result, "Y")
        # Should fall back to simple mark
        mock_ddb.update_item.assert_called_once()

    @patch.dict(os.environ, {"TABLE_NAME": "test_table"})
    def test_no_event_time_skips_checkpoint(self):
        mock_ddb = MagicMock()
        lf._clients["ddb_client"] = mock_ddb

        result = lf.mark_event_and_update_checkpoint("event-789", None)
        self.assertEqual(result, "Y")

        call_args = mock_ddb.transact_write_items.call_args
        items = call_args[1]["TransactItems"] if "TransactItems" in call_args[1] else call_args[0][0]
        self.assertEqual(len(items), 1)  # only event update, no checkpoint


class TestMarkProcessedWithEventTime(unittest.TestCase):
    """Test that _mark_processed routes to checkpoint when event_time is provided."""

    def test_with_event_time_uses_checkpoint(self):
        mock_table = MagicMock()
        mock_ddb = MagicMock()
        lf._clients["ddb_client"] = mock_ddb
        lf._config = MagicMock()
        lf._config.__getitem__ = MagicMock(return_value={"source_region": "us-east-1"})

        response = {"ResponseMetadata": {"HTTPStatusCode": 200}, "Failures": []}
        with patch.dict(os.environ, {"TABLE_NAME": "test_table"}):
            result = lf._mark_processed(mock_table, "evt-1", response, event_time="20251105")

        self.assertEqual(result, "Y")
        mock_ddb.transact_write_items.assert_called_once()

    def test_without_event_time_uses_simple_update(self):
        mock_table = MagicMock()
        response = {"ResponseMetadata": {"HTTPStatusCode": 200}, "Failures": []}

        result = lf._mark_processed(mock_table, "evt-2", response, event_time=None)
        self.assertEqual(result, "Y")
        mock_table.update_item.assert_called_once()

    def test_none_response_returns_N(self):
        mock_table = MagicMock()
        result = lf._mark_processed(mock_table, "evt-3", None)
        self.assertEqual(result, "N")

    def test_failed_response_returns_N(self):
        mock_table = MagicMock()
        response = {"ResponseMetadata": {"HTTPStatusCode": 200}, "Failures": [{"ErrorCode": "AccessDenied"}]}
        result = lf._mark_processed(mock_table, "evt-4", response)
        self.assertEqual(result, "N")

    def tearDown(self):
        lf._clients.clear()
        lf._config = None


if __name__ == "__main__":
    unittest.main()
