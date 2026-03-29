"""Tests for realtime/shared/config_loader.py."""

import os
import sys
import unittest
from configparser import ConfigParser
from unittest.mock import MagicMock, patch

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "realtime", "shared"))

# Mock boto3 before importing
sys.modules.setdefault("boto3", MagicMock())

import config_loader  # noqa: E402


class TestGetConfig(unittest.TestCase):
    """Test get_config loads and parses INI from S3."""

    def setUp(self):
        # Clear the lru_cache between tests
        config_loader.get_config.cache_clear()

    @patch.dict(os.environ, {}, clear=True)
    def test_raises_when_no_bucket_or_key(self):
        with self.assertRaises(ValueError) as ctx:
            config_loader.get_config()
        self.assertIn("bucket and key must be provided", str(ctx.exception))

    @patch.dict(os.environ, {"config_file_bucket": "b"}, clear=True)
    def test_raises_when_key_missing(self):
        with self.assertRaises(ValueError):
            config_loader.get_config()

    @patch.dict(os.environ, {"config_file_key": "k"}, clear=True)
    def test_raises_when_bucket_missing(self):
        with self.assertRaises(ValueError):
            config_loader.get_config()

    @patch("config_loader.boto3")
    def test_loads_from_explicit_params(self, mock_boto3):
        ini_content = "[Section]\nkey = value\n"
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=ini_content.encode("utf-8")))
        }

        result = config_loader.get_config(s3_bucket="mybucket", s3_key="mykey")

        mock_s3.get_object.assert_called_once_with(Bucket="mybucket", Key="mykey")
        self.assertIsInstance(result, ConfigParser)
        self.assertEqual(result["Section"]["key"], "value")

    @patch("config_loader.boto3")
    @patch.dict(os.environ, {"config_file_bucket": "env-bucket", "config_file_key": "env-key"})
    def test_loads_from_env_vars(self, mock_boto3):
        config_loader.get_config.cache_clear()
        ini_content = "[MySection]\nfoo = bar\n"
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=ini_content.encode("utf-8")))
        }

        result = config_loader.get_config()

        mock_s3.get_object.assert_called_once_with(Bucket="env-bucket", Key="env-key")
        self.assertEqual(result["MySection"]["foo"], "bar")

    @patch("config_loader.boto3")
    def test_result_is_cached(self, mock_boto3):
        config_loader.get_config.cache_clear()
        ini_content = "[S]\nk = v\n"
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=ini_content.encode("utf-8")))
        }

        r1 = config_loader.get_config(s3_bucket="b", s3_key="k")
        r2 = config_loader.get_config(s3_bucket="b", s3_key="k")

        self.assertIs(r1, r2)
        # S3 should only be called once due to caching
        mock_s3.get_object.assert_called_once()


class TestGetTableName(unittest.TestCase):
    """Test get_table_name env var lookup."""

    @patch.dict(os.environ, {"TABLE_NAME": "custom_table"})
    def test_returns_env_var(self):
        self.assertEqual(config_loader.get_table_name(), "custom_table")

    @patch.dict(os.environ, {}, clear=True)
    def test_returns_default(self):
        self.assertEqual(config_loader.get_table_name(), "glue_lf_events")


if __name__ == "__main__":
    unittest.main()
