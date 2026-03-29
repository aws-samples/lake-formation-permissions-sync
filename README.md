# Glue Catalog and Lake Formation Permissions Replication

Cross-region replication for AWS Glue Catalog objects, Lake Formation permissions, and Iceberg metadata. Supports both one-time batch sync and continuous realtime replication via CloudTrail events.

## Features

- **Batch mode** — Full catalog bootstrap via AWS Glue ETL. Replicates all databases, tables, partitions, and Lake Formation permissions to a target region.
- **Realtime mode** — Continuous replication via CloudTrail event streaming. Picks up Glue and Lake Formation changes within minutes.
- **Iceberg support** — Remaps `metadata_location`, `previous_metadata_location`, and rewrites Iceberg metadata JSON files in S3 to point to target-region buckets.
- **S3 location remapping** — Rewrites `StorageDescriptor.Location`, `AdditionalLocations`, and all S3 URIs (including `s3a://`) for the target region.
- **CloudTrail fallback** — Handles oversized CloudTrail events (>256KB) by fetching table definitions directly from the source Glue service.
- **Checkpoint with high-water-mark** — DynamoDB transactional checkpoint handles out-of-order CloudTrail events without manual intervention.
- **Monitoring dashboard** — Live Streamlit dashboard showing sync status, events, errors, and configuration.

## Project Structure

```
lake-formation-permissions-sync/
├── config/                         # Configuration (single source of truth)
│   └── glue_config.conf            #   All settings for batch + realtime
├── batch/                          # Batch sync (Glue ETL job)
│   ├── script/app.py               #   Main batch processing script
│   └── infra/                      #   CDK stacks for Glue job + LF admin role
├── realtime/                       # Realtime sync (Lambda + CloudTrail)
│   ├── event_collector/            #   Lambda: pulls CloudTrail events into DynamoDB
│   ├── event_replicator/           #   Lambda: replays events to target region
│   ├── target_admin_setup/         #   Lambda: sets up LF admin in target region
│   ├── infra/                      #   CDK stack for Lambda deployment
│   └── shared/                     #   Shared config utilities
├── dashboard/                      # Monitoring UI
│   └── dashboard.py                #   Streamlit live dashboard
├── tests/                          # Unit tests (182 tests)
├── pyproject.toml                  # Project metadata + uv/pip config
├── requirements.txt                # Python dependencies
└── requirements-dev.txt            # Dev dependencies (testing, linting)
```

## Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured for your source region.
- [AWS CDK](https://aws.amazon.com/cdk/) v2 installed and bootstrapped.
- Python 3.10+.
- An AWS account with Lake Formation admin permissions in both source and target regions.

## Quick Start

### 1. Install dependencies

Using pip:
```bash
pip install -r requirements.txt
```

Using uv (recommended):
```bash
uv sync
```

For development (includes pytest, pytest-cov, mypy, flake8, black, isort):
```bash
pip install -r requirements-dev.txt
# or
uv sync --extra dev
```

### 2. Configure

Edit [`config/glue_config.conf`](./config/glue_config.conf) with your source/target regions, database list, S3 bucket mappings, and CloudTrail lookup window. This single configuration file is shared by both batch and realtime modes. See [Configuration Options](#configuration-options) for details.

### 3. Deploy and run batch sync (bootstrap)

The batch job is the default way to bootstrap your target region. It performs a full one-time replication of all Glue catalog objects and Lake Formation permissions.

```bash
cd batch/infra/

# First time only — bootstrap CDK
cdk bootstrap \
  --context config_bucket_name="YOUR-CONFIG-BUCKET" \
  --context backup_bucket_name="YOUR-BACKUP-BUCKET" \
  --context target_region="us-west-2" \
  --all

# Deploy the stack
cdk deploy \
  --context config_bucket_name="YOUR-CONFIG-BUCKET" \
  --context backup_bucket_name="YOUR-BACKUP-BUCKET" \
  --context target_region="us-west-2" \
  --all
```

Replace `config_bucket_name` with the S3 bucket that holds `glue_config.conf`, `backup_bucket_name` with the bucket for catalog backup JSON files, and `target_region` with the target AWS region.

After the CDK deployment completes, run the Glue job from the AWS Glue Studio console using the **Run** button, or trigger it via the CLI:

```bash
aws glue start-job-run --job-name LFRestoreOnDemandGlueJob
```

The job reads `glue_config.conf` from S3, extracts databases, tables, partitions, and Lake Formation permissions from the source region, and replicates them to the target region.

### 4. Deploy realtime sync (continuous)

Once the batch bootstrap is complete, deploy the realtime stack to keep the target region in sync with ongoing changes:

```bash
cd realtime/infra/

# First time only — bootstrap CDK
cdk bootstrap \
  --context config_file_key="config/glue_config.conf" \
  --context config_file_bucket="YOUR-CONFIG-BUCKET" \
  --context target_region="us-west-2" \
  --context eventbridge_schedule_min="1" \
  --all

# Deploy the stack
cdk deploy \
  --context config_file_key="config/glue_config.conf" \
  --context config_file_bucket="YOUR-CONFIG-BUCKET" \
  --context target_region="us-west-2" \
  --context eventbridge_schedule_min="1" \
  --all
```

To override the Lambda runtime (default: `python3.13`), pass `--context lambda_runtime=python3.12` (supports `python3.10` through `python3.13`).

To pass a named AWS profile, add `--profile <aws_profile>` to any CDK command.

### 5. Run the dashboard

```bash
cd dashboard/
streamlit run dashboard.py
```

The dashboard connects live to DynamoDB using your AWS credentials and shows event activity, processing status, errors, and full configuration visibility.

## Batch Mode

The batch job performs a full one-time replication of Glue catalog objects and Lake Formation permissions to a target region via an AWS Glue ETL job. This is the required first step — run the batch sync to bootstrap the target region before enabling realtime replication.

![Lake Formation Batch](img/LakeFormationDRBatch.png)

## Realtime Mode

Suitable for continuous replication of ongoing changes once the initial batch sync is complete.

![Lake Formation Realtime](img/LakeFormationDRRealTime.png)

Currently replicated events:

- CreateDatabase, UpdateDatabase, DeleteDatabase
- CreateTable, UpdateTable, DeleteTable
- CreatePartition, BatchCreatePartition
- GrantPermissions, RevokePermissions
- BatchGrantPermissions, BatchRevokePermissions
- CreateLFTag, DeleteLFTag
- RegisterResource, DeregisterResource
- PutDataLakeSettings, AddLFTagsToResource

The following event is not supported due to CloudTrail request limitations: CreateDataCellsFilter.

This deployment creates:

- A Lambda function (`event_collector`) to pull records from CloudTrail
- An EventBridge rule to trigger the collector on a configurable schedule
- A DynamoDB table to store pulled CloudTrail records
- A Lambda function (`event_replicator`) to process DynamoDB stream records and replay them to the target region
- An SQS dead-letter queue for failed events
- IAM roles with least-privilege policies for CloudTrail, Glue, Lake Formation (16 specific actions, no wildcards), and DynamoDB access

## Configuration Options

The config file uses INI format with these key sections:

| Section | Key | Description |
|---|---|---|
| `Operation` | `sync_glue_catalog` | Enable/disable Glue catalog replication |
| `Operation` | `sync_lf_permissions` | Enable/disable Lake Formation permissions replication |
| `Operation` | `delete_target_catalog_objects` | Delete objects in target that don't exist in source |
| `Target_s3_update` | `update_table_s3_location` | Remap S3 locations in table definitions |
| `Target_s3_update` | `rewrite_iceberg_metadata` | Rewrite Iceberg metadata JSON files in S3 |
| `AwsDataCatalog` | `source_region` | Source AWS region |
| `AwsDataCatalog` | `destination_region` | Target AWS region |
| `AwsDataCatalog` | `database_list` | Python list of databases to replicate (or `['ALL_DATABASE']`) |
| `AwsDataCatalog` | `S3BucketMapping` / `target_s3_locations` | Python dict mapping source buckets to target buckets |
| `AwsDataCatalog` | `cloudtrail_lookup_hour_duration` | Hours of CloudTrail history to scan (realtime only) |

## Testing

Run the full test suite (182 tests) with coverage:

```bash
python -m pytest tests/ -v
```

Coverage is configured in `pyproject.toml` and runs automatically (minimum threshold: 70%). To run without coverage:

```bash
python -m pytest tests/ -v --no-cov
```

Run a specific test file:

```bash
python -m pytest tests/test_batch_app.py -v
python -m pytest tests/test_batch_app_comprehensive.py -v
python -m pytest tests/test_realtime_lambda.py -v
python -m pytest tests/test_cloudtrail_to_boto3.py -v
python -m pytest tests/test_event_collector.py -v
python -m pytest tests/test_target_admin_setup.py -v
python -m pytest tests/test_config_loader.py -v
python -m pytest tests/test_cdk_stacks.py -v
```

## Type Checking

```bash
mypy batch/script/app.py realtime/
```

mypy is configured in `pyproject.toml` for Python 3.10 with `ignore_missing_imports` enabled.

## Linting

```bash
flake8 --max-line-length=120 batch/script/app.py realtime/ tests/ dashboard/
black --check --line-length=120 .
isort --check --profile=black --line-length=120 .
```

## Clean Up

**Batch stack:**

```bash
cd batch/infra/
cdk destroy \
  --context config_bucket_name="YOUR-CONFIG-BUCKET" \
  --context backup_bucket_name="YOUR-BACKUP-BUCKET" \
  --context target_region="us-west-2" \
  --all
```

**Realtime stack:**

```bash
cd realtime/infra/
cdk destroy \
  --context config_file_key="config/glue_config.conf" \
  --context config_file_bucket="YOUR-CONFIG-BUCKET" \
  --context target_region="us-west-2" \
  --context eventbridge_schedule_min="1" \
  --all
```

The bootstrapping stack created through `cdk bootstrap` is retained. To fully clean up, delete the `CDKToolkit` stack via the CloudFormation console and empty the associated S3 bucket.

## Verifying the Setup

1. Create a database in the source region from the AWS Glue console. The source and target regions are configured in the [configuration file](./config/glue_config.conf).

   ![Create Database](img/GlueCreateDatabase.png)
   ![Database in source region](img/GlueDatabaseNVirginia.png)

2. Check the DynamoDB table `glue_lf_events` for the CreateDatabase event. The `Processed` flag should be `Y`, indicating successful replication.

   ![DynamoDB Event Entry](img/DynamoDBEventEntry.png)

3. Verify the database exists in the target region.

   ![Database in target region](img/GlueDatabaseOregon.png)

## FAQ

**Can I replicate changes to another AWS account?**

Lake Formation permissions are tightly coupled with IAM roles. This utility replays the original API calls without modification, so the target region must have IAM roles with the same names. Cross-account replication is possible if the underlying IAM permissions are managed, but is not supported out of the box.

**How do I report bugs or request enhancements?**

Open an issue on this repository.

## Security

See [CONTRIBUTING](CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
