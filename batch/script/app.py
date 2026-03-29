"""Batch replication of AWS Glue Catalog objects and Lake Formation permissions.

Extracts databases, tables, partitions, and LF permissions from a source region,
writes them to an S3 staging file, then restores them in a target region. Designed
to run as an AWS Glue ETL job.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import sys
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser
from datetime import date, datetime
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import awswrangler as wr
import boto3
from awsglue.utils import getResolvedOptions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RESTORE_WORKERS = 10

INFORMATION_SCHEMA_NAME = "information_schema"

DF_INDEX = ["table_schema", "table_name"]

# Metadata keys returned by the Glue API that must be stripped before writing
# back (they are read-only / server-managed).
_DB_STRIP_KEYS = ("CreateTime", "CatalogId", "VersionId")

_TABLE_STRIP_KEYS = (
    "CatalogId",
    "DatabaseName",
    "LastAccessTime",
    "CreateTime",
    "UpdateTime",
    "CreatedBy",
    "IsRegisteredWithLakeFormation",
    "IsMultiDialectView",
    "IsMaterializedView",
    "VersionId",
)

_PARTITION_STRIP_KEYS = ("CatalogId", "DatabaseName", "CreationTime", "LastAccessTime")

# Keys returned by list_permissions() that are not valid grant_permissions() input.
_PERMISSION_STRIP_KEYS = ("LastUpdated", "LastUpdatedBy", "AdditionalDetails")


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------


def _json_default(obj: Any) -> str:
    """Handle datetime objects that appear in Glue API responses."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


def _dumps(obj: Any, **kwargs: Any) -> str:
    """Thin wrapper around ``json.dumps`` with datetime support."""
    kwargs.setdefault("default", _json_default)
    return json.dumps(obj, **kwargs)


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------


def get_config(s3_config_bucket: str, s3_config_file: str) -> ConfigParser:
    """Load and parse an INI config file from S3."""
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)["Body"].read().decode("utf-8")
    config = ConfigParser()
    config.read_string(body)
    return config


def get_client(region_name: str, service: str):
    """Return a boto3 client for *service* in *region_name*."""
    return boto3.Session(region_name=region_name).client(service)


# ---------------------------------------------------------------------------
# S3 location remapping
# ---------------------------------------------------------------------------


def update_location(s3_location: Optional[str], table_s3_mapping: dict[str, str]) -> Optional[str]:
    """Remap an S3 URI from a source bucket to a target bucket.

    Uses ``urlparse`` so only exact bucket-name matches are replaced
    (no partial-string surprises).
    """
    if not s3_location:
        return s3_location
    parsed = urlparse(s3_location)
    if parsed.netloc in table_s3_mapping:
        parsed = parsed._replace(netloc=table_s3_mapping[parsed.netloc])
        return urlunparse(parsed)
    return s3_location


def update_database_location(database_data: dict, table_s3_mapping: dict[str, str]) -> dict:
    """Remap ``LocationUri`` in a database definition."""
    if "LocationUri" in database_data:
        database_data["LocationUri"] = update_location(database_data["LocationUri"], table_s3_mapping)
    return database_data


def update_table_location(table_data: dict, table_s3_mapping: dict[str, str]) -> dict:
    """Remap every S3 location inside a table definition.

    Handles StorageDescriptor.Location, AdditionalLocations, and Iceberg
    metadata_location / previous_metadata_location parameters.
    """
    sd = table_data.get("StorageDescriptor", {})

    if "Location" in sd:
        sd["Location"] = update_location(sd["Location"], table_s3_mapping)

    additional = sd.get("AdditionalLocations", [])
    if additional:
        sd["AdditionalLocations"] = [update_location(loc, table_s3_mapping) for loc in additional]

    params = table_data.get("Parameters", {})
    for key in ("metadata_location", "previous_metadata_location"):
        if key in params:
            params[key] = update_location(params[key], table_s3_mapping)

    return table_data


def rewrite_iceberg_metadata(
    metadata_s3_uri: Optional[str],
    table_s3_mapping: dict[str, str],
    source_region: str,
) -> Optional[str]:
    """Download an Iceberg metadata JSON from S3, rewrite embedded bucket
    references, and upload to the target bucket.

    Returns the new S3 URI (in the target bucket).
    """
    if not metadata_s3_uri:
        return metadata_s3_uri

    parsed = urlparse(metadata_s3_uri)
    source_bucket = parsed.netloc
    if source_bucket not in table_s3_mapping:
        logger.info("Iceberg metadata bucket %s not in mapping — skipping rewrite", source_bucket)
        return metadata_s3_uri

    target_bucket = table_s3_mapping[source_bucket]
    s3_client = get_client(source_region, "s3")

    try:
        key = parsed.path.lstrip("/")
        resp = s3_client.get_object(Bucket=source_bucket, Key=key)
        content = resp["Body"].read().decode("utf-8")

        for src_bkt, tgt_bkt in table_s3_mapping.items():
            content = content.replace(f"s3://{src_bkt}/", f"s3://{tgt_bkt}/")
            content = content.replace(f"s3a://{src_bkt}/", f"s3a://{tgt_bkt}/")

        s3_client.put_object(
            Bucket=target_bucket, Key=key, Body=content.encode("utf-8"), ContentType="application/json"
        )
        new_uri = f"s3://{target_bucket}/{key}"
        logger.info("Rewrote Iceberg metadata: %s -> %s", metadata_s3_uri, new_uri)
        return new_uri

    except Exception as exc:
        logger.error("Failed to rewrite Iceberg metadata %s: %s", metadata_s3_uri, exc)
        return update_location(metadata_s3_uri, table_s3_mapping)


# ---------------------------------------------------------------------------
# Glue catalog operations
# ---------------------------------------------------------------------------


def _strip_keys(data: dict, keys: tuple[str, ...]) -> dict:
    """Remove server-managed metadata keys from a Glue API response dict."""
    for key in keys:
        data.pop(key, None)
    return data


def create_database(glue_client, database_input: dict) -> None:
    """Create a database in the target region, or update if it already exists."""
    try:
        glue_client.create_database(DatabaseInput=database_input)
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_database(DatabaseInput=database_input, Name=database_input["Name"])


def create_table(glue_client, db_name: str, table_input: dict) -> None:
    """Create a table in the target region, or update if it already exists."""
    _strip_keys(table_input, ("IsMultiDialectView",))
    try:
        glue_client.create_table(DatabaseName=db_name, TableInput=table_input)
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=db_name, TableInput=table_input)


def create_or_update_partition(
    glue_client,
    db_name: str,
    table_name: str,
    partition_data: dict,
    remap_s3: bool,
    table_s3_mapping: dict[str, str],
) -> None:
    """Create or update a single partition."""
    if remap_s3:
        partition_data = update_table_location(partition_data, table_s3_mapping)

    partition_input = {
        "Values": partition_data.get("Values", []),
        "StorageDescriptor": partition_data.get("StorageDescriptor", {}),
        "Parameters": partition_data.get("Parameters", {}),
    }
    try:
        glue_client.create_partition(DatabaseName=db_name, TableName=table_name, PartitionInput=partition_input)
        logger.info("Created partition %s.%s %s", db_name, table_name, partition_data["Values"])
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_partition(
            DatabaseName=db_name,
            TableName=table_name,
            PartitionValueList=partition_data["Values"],
            PartitionInput=partition_input,
        )
        logger.info("Updated partition %s.%s %s", db_name, table_name, partition_data["Values"])


# ---------------------------------------------------------------------------
# Lake Formation permissions
# ---------------------------------------------------------------------------


def _get_database_name_from_permission(row: dict) -> Optional[str]:
    """Extract the database name from a Lake Formation permission record."""
    resource = row.get("Resource", {})
    resource_keys = list(resource)
    lookup = {
        "Database": lambda r: r.get("Database", {}).get("Name"),
        "Table": lambda r: r.get("Table", {}).get("DatabaseName"),
        "TableWithColumns": lambda r: r.get("TableWithColumns", {}).get("DatabaseName"),
    }
    for key in resource_keys:
        if key in lookup:
            return lookup[key](resource)
    return None


def get_permissions(lf_client) -> list[dict]:
    """Paginate through all Lake Formation permissions."""
    logger.info("Fetching Lake Formation permissions")
    permissions: list[dict] = []
    kwargs: dict[str, str] = {}
    while True:
        resp = lf_client.list_permissions(**kwargs)
        permissions.extend(resp["PrincipalResourcePermissions"])
        token = resp.get("NextToken")
        if not token:
            break
        kwargs["NextToken"] = token
    logger.info("Fetched %d permission entries", len(permissions))
    return permissions


def _store_permissions_to_s3(
    permission_data: list[dict],
    region: str,
    bucket: str,
    folder: str,
    filename: str,
) -> None:
    """Serialize permission records to NDJSON and upload to S3."""
    s3_path = f"s3://{bucket}/{folder}/{region}/{filename}"
    logger.info("Writing %d permission records to %s", len(permission_data), s3_path)

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
        for record in permission_data:
            tmp.write(_dumps(record) + "\n")
        tmp_path = tmp.name

    try:
        with open(tmp_path, "rb") as fh:
            wr.s3.upload(local_file=fh, path=s3_path)
    finally:
        os.remove(tmp_path)


def _normalize_all_tables_wildcard(row: dict) -> dict:
    """Convert ALL_TABLES sentinel into a TableWildcard for grant_permissions()."""
    resource = row.get("Resource", {})

    if "Table" in resource:
        table = resource["Table"]
        if table.get("Name") == "ALL_TABLES":
            del table["Name"]
            table["TableWildcard"] = {}

    if "TableWithColumns" in resource:
        twc = resource["TableWithColumns"]
        if twc.get("Name") == "ALL_TABLES":
            resource["Table"] = {k: v for k, v in twc.items() if k not in ("Name", "ColumnWildcard")}
            resource["Table"]["TableWildcard"] = {}
            del resource["TableWithColumns"]

    return row


def _apply_permissions(
    destination_client,
    db_list: list[str],
    source_region: str,
    bucket: str,
    folder: str,
    filename: str,
) -> None:
    """Download permission records from S3 and grant them in the target region."""
    logger.info("Applying permissions from s3://%s/%s/%s/%s", bucket, folder, source_region, filename)

    s3_client = get_client(source_region, "s3")
    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp:
        s3_client.download_fileobj(bucket, f"{folder}/{source_region}/{filename}", tmp)
        tmp_path = tmp.name

    applied = skipped = failed = 0
    try:
        with open(tmp_path, "r") as fh:
            for line in fh:
                row = json.loads(line)
                _strip_keys(row, _PERMISSION_STRIP_KEYS)

                db_name = _get_database_name_from_permission(row)
                if db_name not in db_list:
                    continue

                row = _normalize_all_tables_wildcard(row)
                try:
                    destination_client.grant_permissions(**row)
                    applied += 1
                except destination_client.exceptions.InvalidInputException as exc:
                    logger.warning("Skipping invalid permission grant: %s", exc)
                    skipped += 1
                except Exception as exc:
                    logger.error("Failed to grant permission: %s", exc)
                    failed += 1
    finally:
        os.remove(tmp_path)

    logger.info("Permissions applied=%d  skipped=%d  failed=%d", applied, skipped, failed)


# ---------------------------------------------------------------------------
# Extract / Restore orchestration
# ---------------------------------------------------------------------------


def _process_restore_line(
    glue_client,
    object_data_line: str,
    update_table_s3_location: bool,
    table_s3_mapping: dict[str, str],
    source_region: Optional[str] = None,
) -> tuple[str, str]:
    """Process a single TSV line from the extract file.

    Returns ``(object_type, database_name)``.
    """
    object_type, db_name, object_name, raw_data = object_data_line.split("\t")
    logger.info("Restoring %s %s.%s", object_type, db_name, object_name)

    data = json.loads(raw_data)

    if object_type == "database":
        if update_table_s3_location:
            data = update_database_location(data, table_s3_mapping)
        create_database(glue_client, data)

    elif object_type == "table":
        if update_table_s3_location:
            data = update_table_location(data, table_s3_mapping)
            metadata_loc = data.get("Parameters", {}).get("metadata_location", "")
            if metadata_loc and source_region:
                try:
                    rewrite_iceberg_metadata(metadata_loc, table_s3_mapping, source_region)
                except Exception as exc:
                    logger.warning("Iceberg metadata rewrite failed for %s.%s: %s", db_name, object_name, exc)
        create_table(glue_client, db_name, data)

    elif object_type == "partition":
        create_or_update_partition(glue_client, db_name, object_name, data, update_table_s3_location, table_s3_mapping)

    return object_type, db_name


def _run_parallel_restore(
    phase_name: str,
    lines: list[str],
    glue_client,
    remap_s3: bool,
    table_s3_mapping: dict[str, str],
    source_region: str,
) -> Counter:
    """Run a batch of restore lines through a thread pool and return counts."""
    counts: Counter = Counter()
    with ThreadPoolExecutor(max_workers=MAX_RESTORE_WORKERS) as pool:
        futures = {
            pool.submit(_process_restore_line, glue_client, line, remap_s3, table_s3_mapping, source_region): line
            for line in lines
        }
        for future in as_completed(futures):
            _, db_name = future.result()
            counts[db_name] += 1
    logger.info("%s restore complete: %s", phase_name, dict(counts))
    return counts


def extract_database(source_region: str, output_s3_path: str, db_list: list[str]) -> None:
    """Extract databases, tables, and partitions from the source Glue catalog to S3."""
    logger.info("Extracting catalog from %s", source_region)
    glue = get_client(source_region, "glue")

    db_count: Counter = Counter()
    table_count: Counter = Counter()
    partition_count: Counter = Counter()

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as tmp:
        for db_page in glue.get_paginator("get_databases").paginate():
            for db in db_page["DatabaseList"]:
                if db_list != ["ALL_DATABASE"] and db["Name"] not in db_list:
                    continue

                logger.info("Extracting database: %s", db["Name"])
                _strip_keys(db, _DB_STRIP_KEYS)
                tmp.write(f"database\t{db['Name']}\t\t{_dumps(db)}\n")
                db_count[db["Name"]] += 1

                for tbl_page in glue.get_paginator("get_tables").paginate(DatabaseName=db["Name"]):
                    for table in tbl_page["TableList"]:
                        _strip_keys(table, _TABLE_STRIP_KEYS)
                        tmp.write(f"table\t{db['Name']}\t{table['Name']}\t{_dumps(table)}\n")
                        table_count[db["Name"]] += 1

                        for part_page in glue.get_paginator("get_partitions").paginate(
                            DatabaseName=db["Name"], TableName=table["Name"]
                        ):
                            for partition in part_page["Partitions"]:
                                _strip_keys(partition, _PARTITION_STRIP_KEYS)
                                tmp.write(f"partition\t{db['Name']}\t{table['Name']}\t{_dumps(partition)}\n")
                                partition_count[db["Name"]] += 1

        tmp_path = tmp.name

    logger.info("Uploading extract to %s", output_s3_path)
    try:
        with open(tmp_path, "rb") as fh:
            wr.s3.upload(local_file=fh, path=output_s3_path)
    finally:
        os.remove(tmp_path)

    total_tables = sum(table_count.values())
    total_partitions = sum(partition_count.values())
    logger.info(
        "Extracted %d databases, %d tables, %d partitions",
        len(db_count),
        total_tables,
        total_partitions,
    )


def restore_data(
    config: ConfigParser,
    data_source: str,
    glue_client,
    remap_s3: bool,
    table_s3_mapping: dict[str, str],
) -> None:
    """Download the extract file from S3 and replay it into the target region.

    Processing order: databases -> tables -> partitions (tables and partitions
    are parallelised within their phase).
    """
    s3_path = config[data_source]["s3_data_path"]
    source_region = config["AwsDataCatalog"]["source_region"]
    logger.info("Restoring catalog from %s", s3_path)

    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp:
        wr.s3.download(path=s3_path, local_file=tmp)
        tmp_path = tmp.name

    try:
        with open(tmp_path, "r") as fh:
            all_lines = fh.readlines()
    finally:
        os.remove(tmp_path)

    # Bucket lines by object type to enforce ordering.
    buckets: dict[str, list[str]] = {"database": [], "table": [], "partition": []}
    for line in all_lines:
        obj_type = line.split("\t", 1)[0]
        if obj_type in buckets:
            buckets[obj_type].append(line)

    # Phase 1: Databases (sequential — small count, must finish before tables).
    db_count: Counter = Counter()
    for line in buckets["database"]:
        _, db_name = _process_restore_line(glue_client, line, remap_s3, table_s3_mapping, source_region)
        db_count[db_name] += 1

    # Phase 2 & 3: Tables then partitions (parallel within each phase).
    table_count = _run_parallel_restore(
        "Table", buckets["table"], glue_client, remap_s3, table_s3_mapping, source_region
    )
    partition_count = _run_parallel_restore(
        "Partition", buckets["partition"], glue_client, remap_s3, table_s3_mapping, source_region
    )

    logger.info(
        "Restore complete: %d databases, %d tables, %d partitions",
        sum(db_count.values()),
        sum(table_count.values()),
        sum(partition_count.values()),
    )


# ---------------------------------------------------------------------------
# Table comparison (for delete_target_catalog_objects)
# ---------------------------------------------------------------------------


def _get_tables_df(source_region: str, data_source: str, db_list: list[str]):
    """Query information_schema via Athena and return a DataFrame of tables."""
    session = boto3.Session(region_name=source_region)
    db_filter = "','".join(db_list)
    query = (
        f"SELECT table_schema, table_name "
        f"FROM information_schema.tables "
        f"WHERE table_schema IN ('{db_filter}') AND table_catalog = LOWER('{data_source}')"
    )
    logger.info("Running Athena query: %s", query)
    return wr.athena.read_sql_query(query, database=INFORMATION_SCHEMA_NAME, ctas_approach=False, boto3_session=session)


def _compare_dataframes(source_df, target_df):
    """Outer-merge two DataFrames and return (matched, source_only, target_only)."""
    merged = source_df.merge(target_df, how="outer", indicator=True)
    return (
        merged[merged["_merge"] == "both"],
        merged[merged["_merge"] == "left_only"],
        merged[merged["_merge"] == "right_only"],
    )


def compare_db_tables(config: ConfigParser, data_source: str):
    """Compare source and target tables via Athena and log the results."""
    source_region = config[data_source]["source_region"]
    destination_region = config[data_source]["destination_region"]
    db_list = ast.literal_eval(config[data_source]["database_list"])

    logger.info("Comparing tables: source=%s  target=%s  databases=%s", source_region, destination_region, db_list)

    source_df = _get_tables_df(source_region, data_source, db_list)
    target_df = _get_tables_df(destination_region, data_source, db_list)
    matched, source_only, target_only = _compare_dataframes(source_df, target_df)

    logger.info("Matched tables: %d", len(matched))
    logger.info("Source-only tables: %d", len(source_only))
    logger.info("Target-only tables: %d", len(target_only))

    return matched, source_only, target_only


def delete_target_tables(config: ConfigParser, data_source: str) -> None:
    """Identify tables in the target that don't exist in the source.

    .. note:: Actual deletion is not yet implemented.
    """
    _matched, _source_only, target_only = compare_db_tables(config, data_source)
    if not target_only.empty:
        logger.warning("Found %d target-only tables — deletion not yet implemented", len(target_only))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    start = time.monotonic()

    args = getResolvedOptions(sys.argv, ["CONFIG_BUCKET", "CONFIG_FILE_KEY"])
    config_bucket = args["CONFIG_BUCKET"]
    config_key = args["CONFIG_FILE_KEY"].lstrip("/")

    logger.info("Loading config from s3://%s/%s", config_bucket, config_key)
    config = get_config(config_bucket, config_key)

    # Read operation flags.
    sync_catalog = config.getboolean("Operation", "sync_glue_catalog")
    sync_permissions = config.getboolean("Operation", "sync_lf_permissions")
    delete_target = config.getboolean("Operation", "delete_target_catalog_objects")
    remap_s3 = config.getboolean("Target_s3_update", "update_table_s3_location")
    table_s3_mapping = ast.literal_eval(config.get("AwsDataCatalog", "target_s3_locations"))

    source_region = config["AwsDataCatalog"]["source_region"]
    target_region = config["AwsDataCatalog"]["destination_region"]
    list_datasource = ast.literal_eval(config.get("ListCatalog", "list_datasource"))

    logger.info("Sources: %s  |  %s -> %s  |  remap_s3=%s", list_datasource, source_region, target_region, remap_s3)

    source_lf = get_client(source_region, "lakeformation")
    target_lf = get_client(target_region, "lakeformation")
    target_glue = get_client(target_region, "glue")

    for data_source in list_datasource:
        db_list = ast.literal_eval(config[data_source]["database_list"])
        output_path = config[data_source]["s3_data_path"]

        lf_bucket = config["LakeFormationPermissions"]["lf_storage_bucket"]
        lf_folder = config["LakeFormationPermissions"]["lf_storage_file_folder"]
        lf_file = config["LakeFormationPermissions"]["lf_storage_file_name"]

        logger.info("Processing data source '%s': databases=%s", data_source, db_list)

        if sync_catalog:
            extract_database(source_region, output_path, db_list)
            restore_data(config, data_source, target_glue, remap_s3, table_s3_mapping)

        if delete_target:
            delete_target_tables(config, data_source)

        if sync_permissions:
            # Snapshot permissions from both regions, then apply source -> target.
            _store_permissions_to_s3(get_permissions(source_lf), source_region, lf_bucket, lf_folder, lf_file)
            _store_permissions_to_s3(get_permissions(target_lf), target_region, lf_bucket, lf_folder, lf_file)
            _apply_permissions(target_lf, db_list, source_region, lf_bucket, lf_folder, lf_file)

    elapsed = time.monotonic() - start
    logger.info("Batch replication finished in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
