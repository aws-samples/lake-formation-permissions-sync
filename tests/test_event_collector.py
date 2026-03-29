"""Tests for realtime/event_collector/lambda_function.py."""

import importlib.util
import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

_COLLECTOR_DIR = os.path.join(os.path.dirname(__file__), "..", "realtime", "event_collector")

# Mock external deps — but make botocore.exceptions.ClientError a real exception
_mock_botocore = MagicMock()


class _FakeClientError(Exception):
    def __init__(self, code="ConditionalCheckFailedException"):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


_mock_botocore.exceptions.ClientError = _FakeClientError

# Save original modules so we can restore boto3/botocore after import
_saved_modules = {}
for _mod_name in ("boto3", "botocore", "botocore.exceptions"):
    if _mod_name in sys.modules:
        _saved_modules[_mod_name] = sys.modules[_mod_name]

sys.modules["boto3"] = MagicMock()
sys.modules["botocore"] = _mock_botocore
sys.modules["botocore.exceptions"] = _mock_botocore.exceptions
sys.modules.setdefault("shared", MagicMock())
sys.modules.setdefault("shared.config_loader", MagicMock())

# Use importlib to avoid collision with replicator's lambda_function
_spec = importlib.util.spec_from_file_location(
    "event_collector_lambda",
    os.path.join(_COLLECTOR_DIR, "lambda_function.py"),
)
collector = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(collector)

# Ensure the module's botocore reference is patched
collector.botocore = _mock_botocore

# Restore boto3/botocore to avoid polluting other test files
for _mod_name, _orig in _saved_modules.items():
    sys.modules[_mod_name] = _orig
for _mod_name in ("boto3", "botocore", "botocore.exceptions"):
    if _mod_name not in _saved_modules and _mod_name in sys.modules:
        del sys.modules[_mod_name]


class TestIsRequestSuccessful(unittest.TestCase):
    """Test CloudTrail event success detection."""

    def test_successful_event(self):
        event = {
            "CloudTrailEvent": json.dumps(
                {"responseElements": {"tableStatus": "ACTIVE"}}
            )
        }
        self.assertTrue(collector.is_request_successful(event))

    def test_failed_event_with_error_code(self):
        event = {
            "CloudTrailEvent": json.dumps(
                {"errorCode": "AccessDeniedException", "responseElements": None}
            )
        }
        self.assertFalse(collector.is_request_successful(event))

    def test_successful_event_null_response_elements(self):
        """Some operations (e.g. DeleteTable) return null responseElements on success."""
        event = {
            "CloudTrailEvent": json.dumps(
                {"responseElements": None}
            )
        }
        self.assertTrue(collector.is_request_successful(event))

    def test_successful_batch_operation_empty_failures(self):
        event = {
            "CloudTrailEvent": json.dumps(
                {"responseElements": {"failures": []}}
            )
        }
        self.assertTrue(collector.is_request_successful(event))

    def test_failed_batch_operation_with_failures(self):
        event = {
            "CloudTrailEvent": json.dumps(
                {"responseElements": {"failures": [{"error": "something"}]}}
            )
        }
        self.assertFalse(collector.is_request_successful(event))


class TestDatetimeEncoder(unittest.TestCase):
    """Test the JSON datetime encoder."""

    def test_encodes_datetime(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        result = json.dumps({"ts": dt}, cls=collector.DatetimeEncoder)
        self.assertIn("2025-06-15", result)

    def test_encodes_regular_types(self):
        result = json.dumps({"key": "value", "num": 42}, cls=collector.DatetimeEncoder)
        parsed = json.loads(result)
        self.assertEqual(parsed["key"], "value")


class TestTrackedEvents(unittest.TestCase):
    """Verify the tracked events set contains all expected events."""

    def test_contains_glue_events(self):
        glue_events = {"CreateDatabase", "UpdateDatabase", "DeleteDatabase",
                       "CreateTable", "UpdateTable", "DeleteTable",
                       "CreatePartition", "BatchCreatePartition"}
        self.assertTrue(glue_events.issubset(collector.TRACKED_EVENTS))

    def test_contains_lf_events(self):
        lf_events = {"GrantPermissions", "RevokePermissions",
                     "BatchGrantPermissions", "BatchRevokePermissions",
                     "CreateLFTag", "DeleteLFTag", "UpdateLFTag",
                     "PutDataLakeSettings", "RegisterResource", "DeregisterResource"}
        self.assertTrue(lf_events.issubset(collector.TRACKED_EVENTS))


class TestLambdaHandler(unittest.TestCase):
    """Test the main lambda_handler function."""

    def setUp(self):
        # Reset lazy globals
        collector._config = None
        collector._ct_client = None
        collector._table = None

    @patch.object(collector, "_get_dynamodb_table")
    @patch.object(collector, "_get_cloudtrail_client")
    @patch.object(collector, "_get_config")
    def test_inserts_tracked_successful_event(self, mock_config, mock_ct, mock_table):
        config = MagicMock()
        config.__getitem__ = lambda s, k: {"cloudtrail_lookup_hour_duration": "1"}
        mock_config.return_value = config

        # Each paginator call gets its own fresh copy of the event (lambda mutates EventTime)
        def _make_event():
            return {
                "EventId": "evt-1",
                "EventName": "CreateTable",
                "EventTime": datetime(2025, 6, 15, 10, 0, 0),
                "CloudTrailEvent": json.dumps({"responseElements": {"tableStatus": "ACTIVE"}}),
            }

        mock_ct_client = MagicMock()
        mock_paginator = MagicMock()
        mock_ct_client.get_paginator.return_value = mock_paginator
        # Return fresh event copies per event source to avoid mutation issues
        mock_paginator.paginate.side_effect = [
            [{"Events": [_make_event()]}],  # glue.amazonaws.com
            [{"Events": [_make_event()]}],  # lakeformation.amazonaws.com
        ]
        mock_ct.return_value = mock_ct_client

        mock_ddb_table = MagicMock()
        mock_table.return_value = mock_ddb_table

        result = collector.lambda_handler({}, {})

        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        # Event should be inserted at least once (second call may be duplicate)
        self.assertGreaterEqual(body["inserted"] + body["duplicate"], 1)

    @patch.object(collector, "_get_dynamodb_table")
    @patch.object(collector, "_get_cloudtrail_client")
    @patch.object(collector, "_get_config")
    def test_skips_untracked_event(self, mock_config, mock_ct, mock_table):
        config = MagicMock()
        config.__getitem__ = lambda s, k: {"cloudtrail_lookup_hour_duration": "1"}
        mock_config.return_value = config

        ct_event = {
            "EventId": "evt-2",
            "EventName": "GetTable",  # Not in TRACKED_EVENTS
            "EventTime": datetime(2025, 6, 15),
            "CloudTrailEvent": json.dumps({"responseElements": {}}),
        }

        mock_paginator = MagicMock()
        mock_ct_client = MagicMock()
        mock_ct_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Events": [ct_event]}]
        mock_ct.return_value = mock_ct_client

        mock_ddb_table = MagicMock()
        mock_table.return_value = mock_ddb_table

        result = collector.lambda_handler({}, {})

        body = json.loads(result["body"])
        self.assertEqual(body["inserted"], 0)
        self.assertGreater(body["skipped"], 0)


if __name__ == "__main__":
    unittest.main()
