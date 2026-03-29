"""Legacy test file — replaced by test_cloudtrail_to_boto3.py.

The original import path was invalid Python (hyphens in module names,
typo 'pemissions'). Tests have been migrated to test_cloudtrail_to_boto3.py
with proper imports and expanded coverage.
"""

# Original broken import:
# from lake-formation-pemissions-sync.realtime.event_replicator
#   .lambda_function import get_s3_table_target_bucket_name
#
# See tests/test_cloudtrail_to_boto3.py for the replacement tests.
