"""Tests for realtime/target_admin_setup/lambda_function.py."""

import json
import os
import sys
import unittest
from configparser import ConfigParser
from unittest.mock import MagicMock, patch

# Add module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "realtime", "target_admin_setup"))

# Create real mock for botocore.errorfactory.ClientError
class _FakeClientError(Exception):
    """Minimal stand-in for botocore ClientError."""

    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


# Save original modules so we can restore botocore/boto3 after loading our module
_saved_modules = {}
for _mod_name in ("boto3", "botocore", "botocore.errorfactory"):
    if _mod_name in sys.modules:
        _saved_modules[_mod_name] = sys.modules[_mod_name]

# Mock external deps for module loading
_mock_botocore_ef = MagicMock()
_mock_botocore_ef.ClientError = _FakeClientError
sys.modules["boto3"] = MagicMock()
sys.modules["botocore"] = MagicMock()
sys.modules["botocore.errorfactory"] = _mock_botocore_ef

# Mock shared.config_loader — the handler imports get_config at call time, so this must persist
_mock_config_loader = MagicMock()
sys.modules["shared"] = MagicMock()
sys.modules["shared.config_loader"] = _mock_config_loader

# Now import the module under test
# Use importlib to avoid collision with event_collector's lambda_function on sys.path
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "target_admin_lambda",
    os.path.join(os.path.dirname(__file__), "..", "realtime", "target_admin_setup", "lambda_function.py"),
)
admin_setup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(admin_setup)

# Patch ClientError to our real exception class
admin_setup.ClientError = _FakeClientError

# Restore botocore/boto3 to avoid polluting other test files
# (shared and shared.config_loader stay mocked — they're needed at runtime by admin_setup)
for _mod_name, _orig in _saved_modules.items():
    sys.modules[_mod_name] = _orig
for _mod_name in ("boto3", "botocore", "botocore.errorfactory"):
    if _mod_name not in _saved_modules and _mod_name in sys.modules:
        del sys.modules[_mod_name]


class TestLambdaHandler(unittest.TestCase):
    """Test the target admin setup handler."""

    @patch.dict(os.environ, {"LAMBDA_IAM_ROLE": "arn:aws:iam::123:role/my-lambda-role"})
    def test_adds_new_admin(self):
        config = ConfigParser()
        config.read_dict({"AwsDataCatalog": {"destination_region": "us-west-2"}})
        _mock_config_loader.get_config.return_value = config

        mock_lf = MagicMock()
        admin_setup.boto3.client.return_value = mock_lf
        mock_lf.get_data_lake_settings.return_value = {
            "DataLakeSettings": {
                "DataLakeAdmins": [
                    {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/existing-admin"}
                ]
            }
        }

        result = admin_setup.lambda_handler({}, {})

        self.assertEqual(result["statusCode"], 200)
        mock_lf.put_data_lake_settings.assert_called_once()
        settings = mock_lf.put_data_lake_settings.call_args[1]["DataLakeSettings"]
        admin_arns = [a["DataLakePrincipalIdentifier"] for a in settings["DataLakeAdmins"]]
        self.assertIn("arn:aws:iam::123:role/my-lambda-role", admin_arns)
        self.assertIn("arn:aws:iam::123:role/existing-admin", admin_arns)

    @patch.dict(os.environ, {"LAMBDA_IAM_ROLE": "arn:aws:iam::123:role/existing-admin"})
    def test_skips_duplicate_admin(self):
        config = ConfigParser()
        config.read_dict({"AwsDataCatalog": {"destination_region": "us-west-2"}})
        _mock_config_loader.get_config.return_value = config

        mock_lf = MagicMock()
        admin_setup.boto3.client.return_value = mock_lf
        mock_lf.get_data_lake_settings.return_value = {
            "DataLakeSettings": {
                "DataLakeAdmins": [
                    {"DataLakePrincipalIdentifier": "arn:aws:iam::123:role/existing-admin"}
                ]
            }
        }

        result = admin_setup.lambda_handler({}, {})

        self.assertEqual(result["statusCode"], 200)
        mock_lf.put_data_lake_settings.assert_not_called()

    @patch.dict(os.environ, {"LAMBDA_IAM_ROLE": "arn:aws:iam::123:role/new-admin"})
    def test_handles_invalid_input_exception(self):
        config = ConfigParser()
        config.read_dict({"AwsDataCatalog": {"destination_region": "us-west-2"}})
        _mock_config_loader.get_config.return_value = config

        mock_lf = MagicMock()
        admin_setup.boto3.client.return_value = mock_lf
        mock_lf.get_data_lake_settings.side_effect = _FakeClientError("InvalidInputException")

        result = admin_setup.lambda_handler({}, {})

        self.assertEqual(result["statusCode"], 200)

    @patch.dict(os.environ, {"LAMBDA_IAM_ROLE": "arn:aws:iam::123:role/new-admin"})
    def test_raises_on_other_client_error(self):
        config = ConfigParser()
        config.read_dict({"AwsDataCatalog": {"destination_region": "us-west-2"}})
        _mock_config_loader.get_config.return_value = config

        mock_lf = MagicMock()
        admin_setup.boto3.client.return_value = mock_lf
        mock_lf.get_data_lake_settings.side_effect = _FakeClientError("AccessDeniedException")

        with self.assertRaises(_FakeClientError):
            admin_setup.lambda_handler({}, {})


if __name__ == "__main__":
    unittest.main()
