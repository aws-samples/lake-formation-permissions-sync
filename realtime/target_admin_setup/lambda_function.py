"""Lambda: Add this Lambda's IAM role as a Lake Formation admin in the target region.

This is a setup step that grants the Lambda execution role admin privileges
in the target region's Lake Formation instance, enabling it to replicate
permissions and catalog objects.

Improvements over the original:
- Lazy client initialization
- Structured logging
- Shared config utility
"""

import json
import logging
import os

import boto3
from botocore.errorfactory import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Add the Lambda IAM role as a Lake Formation admin in the target region."""
    from shared.config_loader import get_config

    config = get_config()
    target_region = config["AwsDataCatalog"]["destination_region"]
    lakeformation_iam_role = os.environ["LAMBDA_IAM_ROLE"]

    logger.info(
        "Adding role %s as LF admin in %s",
        lakeformation_iam_role,
        target_region,
    )

    lf_client = boto3.client("lakeformation", region_name=target_region)

    try:
        response = lf_client.get_data_lake_settings()
        admins = response["DataLakeSettings"]["DataLakeAdmins"]

        # Avoid duplicate entries
        existing_arns = {a["DataLakePrincipalIdentifier"] for a in admins}
        if lakeformation_iam_role not in existing_arns:
            admins.append({"DataLakePrincipalIdentifier": lakeformation_iam_role})
            lf_client.put_data_lake_settings(DataLakeSettings=response["DataLakeSettings"])
            logger.info("Role added as LF admin successfully")
        else:
            logger.info("Role is already an LF admin, skipping")

    except ClientError as err:
        error_code = err.response["Error"]["Code"]
        if error_code == "InvalidInputException":
            logger.error("PutDataLakeSettings InvalidInputException: %s", err)
        else:
            raise

    return {
        "statusCode": 200,
        "body": json.dumps("Set LF Administrator in target region!"),
    }
