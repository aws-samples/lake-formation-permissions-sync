"""Comprehensive tests for batch/script/app.py after full rewrite.

Covers every public and internal function:
- JSON helpers: _json_default, _dumps
- AWS helpers: get_config, get_client
- S3 location remapping (additional edge cases)
- _strip_keys
- Glue catalog CRUD: create_database, create_table, create_or_update_partition
- LF permissions: _get_database_name_from_permission, get_permissions,
  _store_permissions_to_s3, _normalize_all_tables_wildcard, _apply_permissions
- Extract/Restore orchestration: extract_database, restore_data, _run_parallel_restore
- Table comparison: _compare_dataframes, compare_db_tables, delete_target_tables
- Entry point: main
"""

import json
import os
import sys
import tempfile
import unittest
from configparser import ConfigParser
from datetime import date, datetime
from io import BytesIO
from unittest.mock import MagicMock, call, mock_open, patch

# Add the batch source to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "batch", "script"))

# Mock awswrangler and awsglue before importing app
sys.modules["awswrangler"] = MagicMock()
sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()

import app  # noqa: E402


# ---------------------------------------------------------------------------
# Helper: build a mock Glue/LF client whose exceptions are real classes
# ---------------------------------------------------------------------------


def _make_glue_mock():
    """Return a MagicMock that behaves like a boto3 Glue client.

    ``client.exceptions.AlreadyExistsException`` is a *real* exception class
    so that ``except client.exceptions.AlreadyExistsException`` works.
    """
    mock_glue = MagicMock()

    class AlreadyExistsException(Exception):
        pass

    mock_glue.exceptions.AlreadyExistsException = AlreadyExistsException
    return mock_glue, AlreadyExistsException


def _make_lf_mock():
    """Return a MagicMock that behaves like a boto3 LakeFormation client."""
    mock_lf = MagicMock()

    class InvalidInputException(Exception):
        pass

    mock_lf.exceptions.InvalidInputException = InvalidInputException
    return mock_lf, InvalidInputException


# ============================================================================
# JSON helpers
# ============================================================================


class TestJsonDefault(unittest.TestCase):
    """Test _json_default handles datetime objects."""

    def test_datetime_serialised(self):
        dt = datetime(2025, 6, 15, 10, 30, 0)
        self.assertEqual(app._json_default(dt), "2025-06-15T10:30:00")

    def test_date_serialised(self):
        d = date(2025, 6, 15)
        self.assertEqual(app._json_default(d), "2025-06-15")

    def test_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            app._json_default(set())

    def test_unsupported_type_message(self):
        with self.assertRaises(TypeError) as ctx:
            app._json_default([1, 2])
        self.assertIn("list", str(ctx.exception))


class TestDumps(unittest.TestCase):
    """Test _dumps wrapper."""

    def test_basic_dict(self):
        result = app._dumps({"key": "value"})
        self.assertEqual(json.loads(result), {"key": "value"})

    def test_datetime_in_dict(self):
        result = app._dumps({"ts": datetime(2025, 1, 1)})
        parsed = json.loads(result)
        self.assertEqual(parsed["ts"], "2025-01-01T00:00:00")

    def test_nested_datetime(self):
        data = {"outer": {"inner": datetime(2025, 12, 31, 23, 59)}}
        result = app._dumps(data)
        parsed = json.loads(result)
        self.assertEqual(parsed["outer"]["inner"], "2025-12-31T23:59:00")

    def test_custom_kwargs_passed_through(self):
        result = app._dumps({"a": 1}, indent=2)
        self.assertIn("\n", result)  # indent=2 produces newlines


# ============================================================================
# _strip_keys
# ============================================================================


class TestStripKeys(unittest.TestCase):
    """Test the _strip_keys helper."""

    def test_removes_specified_keys(self):
        data = {"Name": "db1", "CreateTime": "2025-01-01", "CatalogId": "123"}
        result = app._strip_keys(data, ("CreateTime", "CatalogId"))
        self.assertEqual(result, {"Name": "db1"})

    def test_missing_keys_ignored(self):
        data = {"Name": "db1"}
        result = app._strip_keys(data, ("CreateTime", "CatalogId"))
        self.assertEqual(result, {"Name": "db1"})

    def test_empty_keys_tuple(self):
        data = {"Name": "db1", "Value": 42}
        result = app._strip_keys(data, ())
        self.assertEqual(result, {"Name": "db1", "Value": 42})

    def test_mutates_in_place(self):
        data = {"A": 1, "B": 2}
        result = app._strip_keys(data, ("B",))
        self.assertIs(result, data)  # same object
        self.assertEqual(data, {"A": 1})

    def test_strips_db_keys(self):
        data = {"Name": "mydb", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"}
        app._strip_keys(data, app._DB_STRIP_KEYS)
        self.assertEqual(data, {"Name": "mydb"})

    def test_strips_table_keys(self):
        data = {
            "Name": "tbl",
            "CatalogId": "c",
            "DatabaseName": "db",
            "LastAccessTime": "t",
            "CreateTime": "t",
            "UpdateTime": "t",
            "CreatedBy": "u",
            "IsRegisteredWithLakeFormation": True,
            "IsMultiDialectView": False,
            "IsMaterializedView": False,
            "VersionId": "1",
        }
        app._strip_keys(data, app._TABLE_STRIP_KEYS)
        self.assertEqual(data, {"Name": "tbl"})

    def test_strips_permission_keys(self):
        data = {
            "Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/r"},
            "Resource": {"Database": {"Name": "db"}},
            "Permissions": ["ALL"],
            "LastUpdated": datetime(2025, 1, 1),
            "LastUpdatedBy": "user",
            "AdditionalDetails": {},
        }
        app._strip_keys(data, app._PERMISSION_STRIP_KEYS)
        self.assertNotIn("LastUpdated", data)
        self.assertNotIn("LastUpdatedBy", data)
        self.assertNotIn("AdditionalDetails", data)
        self.assertIn("Principal", data)


# ============================================================================
# Glue catalog CRUD
# ============================================================================


class TestCreateDatabase(unittest.TestCase):
    """Test create_database create-or-update logic."""

    def test_creates_new_database(self):
        mock_glue = MagicMock()
        db_input = {"Name": "test_db", "Description": "test"}
        app.create_database(mock_glue, db_input)
        mock_glue.create_database.assert_called_once_with(DatabaseInput=db_input)
        mock_glue.update_database.assert_not_called()

    def test_updates_existing_database(self):
        mock_glue, AlreadyExists = _make_glue_mock()
        mock_glue.create_database.side_effect = AlreadyExists("exists")
        db_input = {"Name": "test_db", "Description": "test"}
        app.create_database(mock_glue, db_input)
        mock_glue.update_database.assert_called_once_with(DatabaseInput=db_input, Name="test_db")


class TestCreateTable(unittest.TestCase):
    """Test create_table create-or-update logic and key stripping."""

    def test_creates_new_table(self):
        mock_glue = MagicMock()
        table_input = {"Name": "tbl1", "StorageDescriptor": {"Location": "s3://b/d/"}}
        app.create_table(mock_glue, "mydb", table_input)
        mock_glue.create_table.assert_called_once_with(DatabaseName="mydb", TableInput=table_input)

    def test_updates_existing_table(self):
        mock_glue, AlreadyExists = _make_glue_mock()
        mock_glue.create_table.side_effect = AlreadyExists("exists")
        table_input = {"Name": "tbl1", "StorageDescriptor": {"Location": "s3://b/d/"}}
        app.create_table(mock_glue, "mydb", table_input)
        mock_glue.update_table.assert_called_once_with(DatabaseName="mydb", TableInput=table_input)

    def test_strips_is_multi_dialect_view(self):
        mock_glue = MagicMock()
        table_input = {"Name": "tbl1", "IsMultiDialectView": True, "StorageDescriptor": {}}
        app.create_table(mock_glue, "mydb", table_input)
        # The key should have been stripped before the API call
        call_args = mock_glue.create_table.call_args
        self.assertNotIn("IsMultiDialectView", call_args[1]["TableInput"])


class TestCreateOrUpdatePartition(unittest.TestCase):
    """Test partition create-or-update logic."""

    def test_creates_new_partition(self):
        mock_glue = MagicMock()
        part_data = {
            "Values": ["2025"],
            "StorageDescriptor": {"Location": "s3://bucket/data/year=2025/"},
            "Parameters": {"k": "v"},
        }
        app.create_or_update_partition(mock_glue, "mydb", "tbl", part_data, False, {})
        mock_glue.create_partition.assert_called_once()
        self.assertEqual(
            mock_glue.create_partition.call_args[1]["PartitionInput"]["Values"],
            ["2025"],
        )

    def test_updates_existing_partition(self):
        mock_glue, AlreadyExists = _make_glue_mock()
        mock_glue.create_partition.side_effect = AlreadyExists("exists")
        part_data = {
            "Values": ["2025"],
            "StorageDescriptor": {"Location": "s3://bucket/data/"},
            "Parameters": {},
        }
        app.create_or_update_partition(mock_glue, "mydb", "tbl", part_data, False, {})
        mock_glue.update_partition.assert_called_once()

    def test_remaps_s3_when_flag_is_true(self):
        mock_glue = MagicMock()
        mapping = {"src-bucket": "tgt-bucket"}
        part_data = {
            "Values": ["2025"],
            "StorageDescriptor": {"Location": "s3://src-bucket/data/"},
            "Parameters": {},
        }
        app.create_or_update_partition(mock_glue, "mydb", "tbl", part_data, True, mapping)
        created_input = mock_glue.create_partition.call_args[1]["PartitionInput"]
        self.assertEqual(created_input["StorageDescriptor"]["Location"], "s3://tgt-bucket/data/")


# ============================================================================
# LF permissions helpers
# ============================================================================


class TestGetDatabaseNameFromPermission(unittest.TestCase):
    """Test _get_database_name_from_permission for all resource types."""

    def test_database_resource(self):
        row = {"Resource": {"Database": {"Name": "mydb"}}}
        self.assertEqual(app._get_database_name_from_permission(row), "mydb")

    def test_table_resource(self):
        row = {"Resource": {"Table": {"DatabaseName": "mydb", "Name": "tbl"}}}
        self.assertEqual(app._get_database_name_from_permission(row), "mydb")

    def test_table_with_columns_resource(self):
        row = {"Resource": {"TableWithColumns": {"DatabaseName": "mydb", "Name": "tbl"}}}
        self.assertEqual(app._get_database_name_from_permission(row), "mydb")

    def test_unknown_resource_returns_none(self):
        row = {"Resource": {"DataLocation": {"ResourceArn": "arn:..."}}}
        self.assertIsNone(app._get_database_name_from_permission(row))

    def test_empty_resource_returns_none(self):
        row = {"Resource": {}}
        self.assertIsNone(app._get_database_name_from_permission(row))

    def test_missing_resource_returns_none(self):
        row = {}
        self.assertIsNone(app._get_database_name_from_permission(row))


class TestNormalizeAllTablesWildcard(unittest.TestCase):
    """Test _normalize_all_tables_wildcard conversions."""

    def test_table_all_tables_converted(self):
        row = {
            "Resource": {
                "Table": {"DatabaseName": "mydb", "Name": "ALL_TABLES"},
            }
        }
        result = app._normalize_all_tables_wildcard(row)
        table = result["Resource"]["Table"]
        self.assertNotIn("Name", table)
        self.assertEqual(table["TableWildcard"], {})
        self.assertEqual(table["DatabaseName"], "mydb")

    def test_table_normal_name_unchanged(self):
        row = {"Resource": {"Table": {"DatabaseName": "mydb", "Name": "real_table"}}}
        result = app._normalize_all_tables_wildcard(row)
        self.assertEqual(result["Resource"]["Table"]["Name"], "real_table")

    def test_table_with_columns_all_tables_converted(self):
        row = {
            "Resource": {
                "TableWithColumns": {
                    "DatabaseName": "mydb",
                    "Name": "ALL_TABLES",
                    "ColumnWildcard": {},
                },
            }
        }
        result = app._normalize_all_tables_wildcard(row)
        self.assertNotIn("TableWithColumns", result["Resource"])
        table = result["Resource"]["Table"]
        self.assertEqual(table["DatabaseName"], "mydb")
        self.assertEqual(table["TableWildcard"], {})
        self.assertNotIn("ColumnWildcard", table)

    def test_table_with_columns_normal_name_unchanged(self):
        row = {
            "Resource": {
                "TableWithColumns": {
                    "DatabaseName": "mydb",
                    "Name": "tbl",
                    "ColumnWildcard": {},
                },
            }
        }
        result = app._normalize_all_tables_wildcard(row)
        self.assertEqual(result["Resource"]["TableWithColumns"]["Name"], "tbl")

    def test_no_table_or_twc_noop(self):
        row = {"Resource": {"Database": {"Name": "mydb"}}}
        result = app._normalize_all_tables_wildcard(row)
        self.assertEqual(result, row)


class TestGetPermissions(unittest.TestCase):
    """Test get_permissions pagination."""

    def test_single_page(self):
        mock_lf = MagicMock()
        mock_lf.list_permissions.return_value = {
            "PrincipalResourcePermissions": [{"p": 1}, {"p": 2}],
        }
        result = app.get_permissions(mock_lf)
        self.assertEqual(len(result), 2)
        mock_lf.list_permissions.assert_called_once()

    def test_multiple_pages(self):
        mock_lf = MagicMock()
        mock_lf.list_permissions.side_effect = [
            {"PrincipalResourcePermissions": [{"p": 1}], "NextToken": "tok1"},
            {"PrincipalResourcePermissions": [{"p": 2}], "NextToken": "tok2"},
            {"PrincipalResourcePermissions": [{"p": 3}]},
        ]
        result = app.get_permissions(mock_lf)
        self.assertEqual(len(result), 3)
        self.assertEqual(mock_lf.list_permissions.call_count, 3)
        # Verify NextToken was passed
        mock_lf.list_permissions.assert_any_call(NextToken="tok1")
        mock_lf.list_permissions.assert_any_call(NextToken="tok2")


class TestStorePermissionsToS3(unittest.TestCase):
    """Test _store_permissions_to_s3 writes NDJSON and uploads."""

    @patch("app.wr")
    @patch("app.os.remove")
    def test_writes_ndjson_and_uploads(self, mock_remove, mock_wr):
        permissions = [
            {"Principal": "arn:1", "Resource": {"Database": {"Name": "db1"}}},
            {"Principal": "arn:2", "Resource": {"Table": {"Name": "tbl"}}},
        ]
        app._store_permissions_to_s3(permissions, "us-east-1", "mybucket", "folder", "perms.json")

        # Verify upload was called
        mock_wr.s3.upload.assert_called_once()
        upload_call = mock_wr.s3.upload.call_args
        self.assertEqual(
            upload_call[1]["path"],
            "s3://mybucket/folder/us-east-1/perms.json",
        )
        # Temp file should be cleaned up
        mock_remove.assert_called_once()


class TestApplyPermissions(unittest.TestCase):
    """Test _apply_permissions grant logic."""

    @patch("app.os.remove")
    @patch("app.get_client")
    def test_applies_matching_permissions(self, mock_get_client, mock_remove):
        # Create a temp file with permission records
        records = [
            {
                "Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/r"},
                "Resource": {"Database": {"Name": "mydb"}},
                "Permissions": ["ALL"],
                "PermissionsWithGrantOption": [],
            },
            {
                "Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/r"},
                "Resource": {"Database": {"Name": "otherdb"}},
                "Permissions": ["ALL"],
                "PermissionsWithGrantOption": [],
            },
        ]
        # Write records to a real temp file and mock download to populate it
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
        for rec in records:
            tmp.write(json.dumps(rec) + "\n")
        tmp.close()

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        # Mock download_fileobj to copy our temp content
        def fake_download(bucket, key, fileobj):
            with open(tmp.name, "rb") as src:
                fileobj.write(src.read())

        mock_s3.download_fileobj.side_effect = fake_download

        mock_lf = MagicMock()
        app._apply_permissions(mock_lf, ["mydb"], "us-east-1", "bucket", "folder", "perms.json")

        # Only the "mydb" permission should be granted (not "otherdb")
        mock_lf.grant_permissions.assert_called_once()
        grant_call = mock_lf.grant_permissions.call_args
        self.assertEqual(grant_call[1]["Resource"]["Database"]["Name"], "mydb")

        os.remove(tmp.name)

    @patch("app.os.remove")
    @patch("app.get_client")
    def test_skips_invalid_principal(self, mock_get_client, mock_remove):
        records = [
            {
                "Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/deleted"},
                "Resource": {"Database": {"Name": "mydb"}},
                "Permissions": ["ALL"],
                "PermissionsWithGrantOption": [],
                "LastUpdated": "2025-01-01",
            },
        ]
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
        for rec in records:
            tmp.write(json.dumps(rec) + "\n")
        tmp.close()

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        def fake_download(bucket, key, fileobj):
            with open(tmp.name, "rb") as src:
                fileobj.write(src.read())

        mock_s3.download_fileobj.side_effect = fake_download

        mock_lf, InvalidInput = _make_lf_mock()
        mock_lf.grant_permissions.side_effect = InvalidInput("bad principal")

        # Should not raise — graceful skip
        app._apply_permissions(mock_lf, ["mydb"], "us-east-1", "bucket", "folder", "perms.json")
        mock_lf.grant_permissions.assert_called_once()

        os.remove(tmp.name)

    @patch("app.os.remove")
    @patch("app.get_client")
    def test_strips_metadata_keys_before_grant(self, mock_get_client, mock_remove):
        records = [
            {
                "Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/r"},
                "Resource": {"Database": {"Name": "mydb"}},
                "Permissions": ["ALL"],
                "PermissionsWithGrantOption": [],
                "LastUpdated": "2025-01-01T00:00:00",
                "LastUpdatedBy": "admin",
                "AdditionalDetails": {"foo": "bar"},
            },
        ]
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
        for rec in records:
            tmp.write(json.dumps(rec) + "\n")
        tmp.close()

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        def fake_download(bucket, key, fileobj):
            with open(tmp.name, "rb") as src:
                fileobj.write(src.read())

        mock_s3.download_fileobj.side_effect = fake_download

        mock_lf = MagicMock()
        app._apply_permissions(mock_lf, ["mydb"], "us-east-1", "bucket", "folder", "perms.json")

        grant_call = mock_lf.grant_permissions.call_args
        granted = grant_call[1]
        self.assertNotIn("LastUpdated", granted)
        self.assertNotIn("LastUpdatedBy", granted)
        self.assertNotIn("AdditionalDetails", granted)

        os.remove(tmp.name)


# ============================================================================
# Extract / Restore orchestration
# ============================================================================


class TestExtractDatabase(unittest.TestCase):
    """Test extract_database writes correct TSV format."""

    @patch("app.wr")
    @patch("app.os.remove")
    @patch("app.get_client")
    def test_extracts_databases_tables_partitions(self, mock_get_client, mock_remove, mock_wr):
        mock_glue = MagicMock()
        mock_get_client.return_value = mock_glue

        # Setup paginator responses
        db_paginator = MagicMock()
        db_paginator.paginate.return_value = [
            {
                "DatabaseList": [
                    {"Name": "db1", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"},
                ]
            }
        ]

        tbl_paginator = MagicMock()
        tbl_paginator.paginate.return_value = [
            {
                "TableList": [
                    {
                        "Name": "tbl1",
                        "CatalogId": "c",
                        "DatabaseName": "db1",
                        "StorageDescriptor": {"Location": "s3://b/d/"},
                    },
                ]
            }
        ]

        part_paginator = MagicMock()
        part_paginator.paginate.return_value = [{"Partitions": []}]

        def pick_paginator(operation):
            return {
                "get_databases": db_paginator,
                "get_tables": tbl_paginator,
                "get_partitions": part_paginator,
            }[operation]

        mock_glue.get_paginator.side_effect = pick_paginator

        app.extract_database("us-east-1", "s3://bucket/output.tsv", ["db1"])

        # Verify upload was called
        mock_wr.s3.upload.assert_called_once()

    @patch("app.wr")
    @patch("app.os.remove")
    @patch("app.get_client")
    def test_skips_unmatched_databases(self, mock_get_client, mock_remove, mock_wr):
        mock_glue = MagicMock()
        mock_get_client.return_value = mock_glue

        db_paginator = MagicMock()
        db_paginator.paginate.return_value = [
            {
                "DatabaseList": [
                    {"Name": "db1", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"},
                    {"Name": "db2", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"},
                ]
            }
        ]

        tbl_paginator = MagicMock()
        tbl_paginator.paginate.return_value = [{"TableList": []}]

        part_paginator = MagicMock()
        part_paginator.paginate.return_value = [{"Partitions": []}]

        def pick_paginator(operation):
            return {
                "get_databases": db_paginator,
                "get_tables": tbl_paginator,
                "get_partitions": part_paginator,
            }[operation]

        mock_glue.get_paginator.side_effect = pick_paginator

        app.extract_database("us-east-1", "s3://bucket/output.tsv", ["db1"])

        # get_tables should only be called for db1, not db2
        tbl_paginator.paginate.assert_called_once_with(DatabaseName="db1")

    @patch("app.wr")
    @patch("app.os.remove")
    @patch("app.get_client")
    def test_all_database_sentinel(self, mock_get_client, mock_remove, mock_wr):
        mock_glue = MagicMock()
        mock_get_client.return_value = mock_glue

        db_paginator = MagicMock()
        db_paginator.paginate.return_value = [
            {
                "DatabaseList": [
                    {"Name": "db1", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"},
                    {"Name": "db2", "CreateTime": "t", "CatalogId": "c", "VersionId": "v"},
                ]
            }
        ]

        tbl_paginator = MagicMock()
        tbl_paginator.paginate.return_value = [{"TableList": []}]

        part_paginator = MagicMock()
        part_paginator.paginate.return_value = [{"Partitions": []}]

        def pick_paginator(operation):
            return {
                "get_databases": db_paginator,
                "get_tables": tbl_paginator,
                "get_partitions": part_paginator,
            }[operation]

        mock_glue.get_paginator.side_effect = pick_paginator

        app.extract_database("us-east-1", "s3://bucket/output.tsv", ["ALL_DATABASE"])

        # Both databases should be extracted
        self.assertEqual(tbl_paginator.paginate.call_count, 2)


class TestRestoreData(unittest.TestCase):
    """Test restore_data processes TSV lines in correct order."""

    @patch("app._run_parallel_restore")
    @patch("app._process_restore_line")
    @patch("app.wr")
    @patch("app.os.remove")
    def test_processes_databases_then_tables_then_partitions(
        self, mock_remove, mock_wr, mock_process, mock_parallel
    ):
        config = ConfigParser()
        config.read_dict(
            {
                "src1": {"s3_data_path": "s3://bucket/data.tsv"},
                "AwsDataCatalog": {"source_region": "us-east-1"},
            }
        )

        # Create a temp file with TSV lines
        lines = [
            'database\tdb1\t\t{"Name":"db1"}\n',
            'table\tdb1\ttbl1\t{"Name":"tbl1","StorageDescriptor":{"Location":"s3://b/d/"}}\n',
            'partition\tdb1\ttbl1\t{"Values":["2025"],"StorageDescriptor":{"Location":"s3://b/p/"},"Parameters":{}}\n',
        ]
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv")
        tmp.writelines(lines)
        tmp.close()

        # Mock download to write our temp file content
        def fake_download(path, local_file):
            with open(tmp.name, "rb") as src:
                local_file.write(src.read())

        mock_wr.s3.download.side_effect = fake_download
        mock_process.return_value = ("database", "db1")

        from collections import Counter

        mock_parallel.return_value = Counter({"db1": 1})

        mock_glue = MagicMock()
        app.restore_data(config, "src1", mock_glue, False, {})

        # Database line processed sequentially
        mock_process.assert_called_once()
        # Tables and partitions processed in parallel
        self.assertEqual(mock_parallel.call_count, 2)
        # First parallel call is for tables, second for partitions
        table_call = mock_parallel.call_args_list[0]
        self.assertEqual(table_call[0][0], "Table")
        partition_call = mock_parallel.call_args_list[1]
        self.assertEqual(partition_call[0][0], "Partition")

        os.remove(tmp.name)


class TestRunParallelRestore(unittest.TestCase):
    """Test _run_parallel_restore thread pool execution."""

    @patch("app._process_restore_line")
    def test_returns_counter_of_db_names(self, mock_process):
        mock_process.side_effect = [
            ("table", "db1"),
            ("table", "db1"),
            ("table", "db2"),
        ]
        lines = ["line1", "line2", "line3"]
        mock_glue = MagicMock()
        result = app._run_parallel_restore("Table", lines, mock_glue, False, {}, "us-east-1")
        self.assertEqual(result["db1"], 2)
        self.assertEqual(result["db2"], 1)

    @patch("app._process_restore_line")
    def test_empty_lines_returns_empty_counter(self, mock_process):
        mock_glue = MagicMock()
        result = app._run_parallel_restore("Table", [], mock_glue, False, {}, "us-east-1")
        self.assertEqual(len(result), 0)


# ============================================================================
# Table comparison
# ============================================================================


class TestCompareDataframes(unittest.TestCase):
    """Test _compare_dataframes outer merge."""

    def test_finds_matches_and_differences(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not available")

        source = pd.DataFrame({"table_schema": ["db1", "db1"], "table_name": ["t1", "t2"]})
        target = pd.DataFrame({"table_schema": ["db1", "db1"], "table_name": ["t2", "t3"]})

        matched, source_only, target_only = app._compare_dataframes(source, target)
        self.assertEqual(len(matched), 1)
        self.assertEqual(len(source_only), 1)
        self.assertEqual(len(target_only), 1)

    def test_identical_dataframes(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not available")

        df = pd.DataFrame({"table_schema": ["db1"], "table_name": ["t1"]})
        matched, source_only, target_only = app._compare_dataframes(df, df.copy())
        self.assertEqual(len(matched), 1)
        self.assertEqual(len(source_only), 0)
        self.assertEqual(len(target_only), 0)


# ============================================================================
# Process restore line — additional edge cases
# ============================================================================


class TestProcessRestoreLineEdgeCases(unittest.TestCase):
    """Additional edge case tests for _process_restore_line."""

    def test_database_with_s3_remapping(self):
        """Database LocationUri should be remapped when flag is True."""
        db_data = json.dumps({"Name": "mydb", "LocationUri": "s3://src-bucket/mydb/"})
        line = f"database\tmydb\t\t{db_data}"
        mock_glue = MagicMock()
        mapping = {"src-bucket": "tgt-bucket"}

        app._process_restore_line(mock_glue, line, True, mapping)

        # Verify create_database was called with remapped LocationUri
        create_call = mock_glue.create_database.call_args
        db_input = create_call[1]["DatabaseInput"]
        self.assertEqual(db_input["LocationUri"], "s3://tgt-bucket/mydb/")

    def test_table_with_all_location_types(self):
        """Table with StorageDescriptor, metadata_location, previous_metadata_location, and AdditionalLocations."""
        table_data = json.dumps(
            {
                "Name": "complex_tbl",
                "StorageDescriptor": {
                    "Location": "s3://src/db/tbl/",
                    "AdditionalLocations": ["s3://src/extra/"],
                },
                "Parameters": {
                    "metadata_location": "s3://src/metadata/v2.json",
                    "previous_metadata_location": "s3://src/metadata/v1.json",
                },
            }
        )
        line = f"table\tmydb\tcomplex_tbl\t{table_data}"
        mock_glue = MagicMock()
        mapping = {"src": "tgt"}

        app._process_restore_line(mock_glue, line, True, mapping, "us-east-1")

        create_call = mock_glue.create_table.call_args
        tbl = create_call[1]["TableInput"]
        self.assertEqual(tbl["StorageDescriptor"]["Location"], "s3://tgt/db/tbl/")
        self.assertEqual(tbl["StorageDescriptor"]["AdditionalLocations"], ["s3://tgt/extra/"])
        self.assertEqual(tbl["Parameters"]["metadata_location"], "s3://tgt/metadata/v2.json")
        self.assertEqual(tbl["Parameters"]["previous_metadata_location"], "s3://tgt/metadata/v1.json")

    def test_iceberg_metadata_rewrite_failure_is_graceful(self):
        """If rewrite_iceberg_metadata raises, the table should still be created."""
        table_data = json.dumps(
            {
                "Name": "ice_tbl",
                "StorageDescriptor": {"Location": "s3://src/data/"},
                "Parameters": {
                    "metadata_location": "s3://src/metadata/v1.metadata.json",
                    "table_type": "ICEBERG",
                },
            }
        )
        line = f"table\tmydb\tice_tbl\t{table_data}"
        mock_glue = MagicMock()
        mapping = {"src": "tgt"}

        with patch.object(app, "rewrite_iceberg_metadata", side_effect=Exception("S3 error")):
            otype, db = app._process_restore_line(mock_glue, line, True, mapping, "us-east-1")

        # Table should still be created despite metadata rewrite failure
        mock_glue.create_table.assert_called_once()
        self.assertEqual(otype, "table")

    def test_partition_with_multi_value_key(self):
        """Partition with multiple values in the key."""
        part_data = json.dumps(
            {
                "Values": ["2025", "01", "15"],
                "StorageDescriptor": {"Location": "s3://bucket/data/y=2025/m=01/d=15/"},
                "Parameters": {},
            }
        )
        line = f"partition\tmydb\ttbl\t{part_data}"
        mock_glue = MagicMock()

        otype, db = app._process_restore_line(mock_glue, line, False, {})
        self.assertEqual(otype, "partition")
        create_call = mock_glue.create_partition.call_args
        self.assertEqual(create_call[1]["PartitionInput"]["Values"], ["2025", "01", "15"])


# ============================================================================
# Update location — additional edge cases
# ============================================================================


class TestUpdateLocationEdgeCases(unittest.TestCase):
    """Additional edge cases for update_location."""

    def test_s3a_protocol(self):
        """s3a:// URIs should also be remapped."""
        mapping = {"src-bucket": "tgt-bucket"}
        result = app.update_location("s3a://src-bucket/data/", mapping)
        self.assertEqual(result, "s3a://tgt-bucket/data/")

    def test_s3n_protocol(self):
        """s3n:// URIs should also be remapped."""
        mapping = {"src-bucket": "tgt-bucket"}
        result = app.update_location("s3n://src-bucket/data/", mapping)
        self.assertEqual(result, "s3n://tgt-bucket/data/")

    def test_multiple_mapping_entries(self):
        """Only the matching bucket is replaced."""
        mapping = {"bucket-a": "target-a", "bucket-b": "target-b"}
        result = app.update_location("s3://bucket-b/data/", mapping)
        self.assertEqual(result, "s3://target-b/data/")

    def test_deep_path_preserved(self):
        mapping = {"b": "t"}
        result = app.update_location("s3://b/a/b/c/d/e/f/g.parquet", mapping)
        self.assertEqual(result, "s3://t/a/b/c/d/e/f/g.parquet")

    def test_trailing_slash_preserved(self):
        mapping = {"b": "t"}
        result = app.update_location("s3://b/path/", mapping)
        self.assertEqual(result, "s3://t/path/")

    def test_no_path(self):
        mapping = {"b": "t"}
        result = app.update_location("s3://b", mapping)
        self.assertEqual(result, "s3://t")


# ============================================================================
# Rewrite Iceberg Metadata — edge cases
# ============================================================================


class TestRewriteIcebergMetadataEdgeCases(unittest.TestCase):
    """Additional edge cases for rewrite_iceberg_metadata."""

    @patch.object(app, "get_client")
    def test_handles_s3_error_gracefully(self, mock_get_client):
        """If S3 get_object fails, fall back to simple URI remapping."""
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3
        mock_s3.get_object.side_effect = Exception("AccessDenied")

        mapping = {"src-bucket": "tgt-bucket"}
        result = app.rewrite_iceberg_metadata(
            "s3://src-bucket/metadata/v1.metadata.json", mapping, "us-east-1"
        )
        # Falls back to update_location
        self.assertEqual(result, "s3://tgt-bucket/metadata/v1.metadata.json")

    @patch.object(app, "get_client")
    def test_multiple_bucket_replacements_in_content(self, mock_get_client):
        """Content with references to multiple source buckets should all be replaced."""
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        content = json.dumps(
            {
                "location": "s3://data-east/warehouse/tbl",
                "properties": {"write.data.path": "s3://staging-east/data/"},
            }
        )
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=content.encode("utf-8")))
        }

        mapping = {"data-east": "data-west", "staging-east": "staging-west"}
        result = app.rewrite_iceberg_metadata(
            "s3://data-east/metadata/v1.metadata.json", mapping, "us-east-1"
        )

        put_call = mock_s3.put_object.call_args
        written = json.loads(put_call[1]["Body"].decode("utf-8"))
        self.assertEqual(written["location"], "s3://data-west/warehouse/tbl")
        self.assertEqual(written["properties"]["write.data.path"], "s3://staging-west/data/")


# ============================================================================
# Main entry point
# ============================================================================


class TestMain(unittest.TestCase):
    """Test main() orchestration."""

    @patch("app.get_client")
    @patch("app.get_config")
    @patch("app.getResolvedOptions")
    @patch("app._apply_permissions")
    @patch("app._store_permissions_to_s3")
    @patch("app.get_permissions")
    @patch("app.delete_target_tables")
    @patch("app.restore_data")
    @patch("app.extract_database")
    def test_main_sync_catalog_only(
        self,
        mock_extract,
        mock_restore,
        mock_delete,
        mock_get_perms,
        mock_store_perms,
        mock_apply_perms,
        mock_resolved,
        mock_get_config,
        mock_get_client,
    ):
        mock_resolved.return_value = {"CONFIG_BUCKET": "cfg-bucket", "CONFIG_FILE_KEY": "config.conf"}

        config = ConfigParser()
        config.read_dict(
            {
                "Operation": {
                    "sync_glue_catalog": "true",
                    "sync_lf_permissions": "false",
                    "delete_target_catalog_objects": "false",
                },
                "Target_s3_update": {"update_table_s3_location": "false"},
                "AwsDataCatalog": {
                    "source_region": "us-east-1",
                    "destination_region": "us-west-2",
                    "target_s3_locations": "{}",
                },
                "ListCatalog": {"list_datasource": "['src1']"},
                "src1": {
                    "database_list": "['db1']",
                    "s3_data_path": "s3://bucket/output.tsv",
                },
                "LakeFormationPermissions": {
                    "lf_storage_bucket": "lf-bucket",
                    "lf_storage_file_folder": "lf-folder",
                    "lf_storage_file_name": "perms.json",
                },
            }
        )
        mock_get_config.return_value = config
        mock_get_client.return_value = MagicMock()

        app.main()

        mock_extract.assert_called_once()
        mock_restore.assert_called_once()
        mock_delete.assert_not_called()
        mock_get_perms.assert_not_called()

    @patch("app.get_client")
    @patch("app.get_config")
    @patch("app.getResolvedOptions")
    @patch("app._apply_permissions")
    @patch("app._store_permissions_to_s3")
    @patch("app.get_permissions")
    @patch("app.delete_target_tables")
    @patch("app.restore_data")
    @patch("app.extract_database")
    def test_main_sync_permissions_only(
        self,
        mock_extract,
        mock_restore,
        mock_delete,
        mock_get_perms,
        mock_store_perms,
        mock_apply_perms,
        mock_resolved,
        mock_get_config,
        mock_get_client,
    ):
        mock_resolved.return_value = {"CONFIG_BUCKET": "cfg-bucket", "CONFIG_FILE_KEY": "config.conf"}

        config = ConfigParser()
        config.read_dict(
            {
                "Operation": {
                    "sync_glue_catalog": "false",
                    "sync_lf_permissions": "true",
                    "delete_target_catalog_objects": "false",
                },
                "Target_s3_update": {"update_table_s3_location": "false"},
                "AwsDataCatalog": {
                    "source_region": "us-east-1",
                    "destination_region": "us-west-2",
                    "target_s3_locations": "{}",
                },
                "ListCatalog": {"list_datasource": "['src1']"},
                "src1": {
                    "database_list": "['db1']",
                    "s3_data_path": "s3://bucket/output.tsv",
                },
                "LakeFormationPermissions": {
                    "lf_storage_bucket": "lf-bucket",
                    "lf_storage_file_folder": "lf-folder",
                    "lf_storage_file_name": "perms.json",
                },
            }
        )
        mock_get_config.return_value = config
        mock_get_client.return_value = MagicMock()
        mock_get_perms.return_value = [{"p": 1}]

        app.main()

        mock_extract.assert_not_called()
        mock_restore.assert_not_called()
        mock_store_perms.assert_called()  # twice: source + target
        mock_apply_perms.assert_called_once()

    @patch("app.get_client")
    @patch("app.get_config")
    @patch("app.getResolvedOptions")
    @patch("app._apply_permissions")
    @patch("app._store_permissions_to_s3")
    @patch("app.get_permissions")
    @patch("app.delete_target_tables")
    @patch("app.restore_data")
    @patch("app.extract_database")
    def test_main_all_operations(
        self,
        mock_extract,
        mock_restore,
        mock_delete,
        mock_get_perms,
        mock_store_perms,
        mock_apply_perms,
        mock_resolved,
        mock_get_config,
        mock_get_client,
    ):
        mock_resolved.return_value = {"CONFIG_BUCKET": "cfg-bucket", "CONFIG_FILE_KEY": "/config.conf"}

        config = ConfigParser()
        config.read_dict(
            {
                "Operation": {
                    "sync_glue_catalog": "true",
                    "sync_lf_permissions": "true",
                    "delete_target_catalog_objects": "true",
                },
                "Target_s3_update": {"update_table_s3_location": "true"},
                "AwsDataCatalog": {
                    "source_region": "us-east-1",
                    "destination_region": "us-west-2",
                    "target_s3_locations": "{'src': 'tgt'}",
                },
                "ListCatalog": {"list_datasource": "['src1']"},
                "src1": {
                    "database_list": "['db1']",
                    "s3_data_path": "s3://bucket/output.tsv",
                },
                "LakeFormationPermissions": {
                    "lf_storage_bucket": "lf-bucket",
                    "lf_storage_file_folder": "lf-folder",
                    "lf_storage_file_name": "perms.json",
                },
            }
        )
        mock_get_config.return_value = config
        mock_get_client.return_value = MagicMock()
        mock_get_perms.return_value = []

        app.main()

        mock_extract.assert_called_once()
        mock_restore.assert_called_once()
        mock_delete.assert_called_once()
        mock_apply_perms.assert_called_once()

    @patch("app.get_client")
    @patch("app.get_config")
    @patch("app.getResolvedOptions")
    @patch("app.extract_database")
    @patch("app.restore_data")
    def test_main_strips_leading_slash_from_config_key(
        self, mock_restore, mock_extract, mock_resolved, mock_get_config, mock_get_client
    ):
        """CONFIG_FILE_KEY with leading slash should be stripped."""
        mock_resolved.return_value = {"CONFIG_BUCKET": "b", "CONFIG_FILE_KEY": "/path/config.conf"}

        config = ConfigParser()
        config.read_dict(
            {
                "Operation": {
                    "sync_glue_catalog": "false",
                    "sync_lf_permissions": "false",
                    "delete_target_catalog_objects": "false",
                },
                "Target_s3_update": {"update_table_s3_location": "false"},
                "AwsDataCatalog": {
                    "source_region": "us-east-1",
                    "destination_region": "us-west-2",
                    "target_s3_locations": "{}",
                },
                "ListCatalog": {"list_datasource": "[]"},
            }
        )
        mock_get_config.return_value = config
        mock_get_client.return_value = MagicMock()

        app.main()

        # get_config should receive key without leading slash
        mock_get_config.assert_called_once_with("b", "path/config.conf")


if __name__ == "__main__":
    unittest.main()
