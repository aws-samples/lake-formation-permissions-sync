"""Shared configuration loader for Lake Formation DR Lambda functions.

Loads config from an S3-hosted .conf file. Uses lazy initialization
so that failures during module import don't permanently break the Lambda.
"""

import logging
import os
from configparser import ConfigParser
from functools import lru_cache

import boto3

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_config(s3_bucket=None, s3_key=None):
    """Load and parse config from S3.

    Uses environment variables if explicit params not provided.
    Results are cached so repeated calls don't hit S3 again.

    Args:
        s3_bucket: S3 bucket containing the config file.
        s3_key: S3 key path to the config file.

    Returns:
        ConfigParser instance with parsed configuration.

    Raises:
        ValueError: If bucket/key not provided and not in environment.
        botocore.exceptions.ClientError: If S3 read fails.
    """
    bucket = s3_bucket or os.environ.get("config_file_bucket")
    key = s3_key or os.environ.get("config_file_key")

    if not bucket or not key:
        raise ValueError(
            "Config bucket and key must be provided via arguments "
            "or environment variables 'config_file_bucket' and 'config_file_key'"
        )

    logger.info("Loading config from s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    config = ConfigParser()
    config.read_string(body)
    return config


def get_table_name():
    """Get the DynamoDB table name from environment or default."""
    return os.environ.get("TABLE_NAME", "glue_lf_events")
