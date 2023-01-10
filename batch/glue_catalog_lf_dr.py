#!/usr/bin/env python
import boto3
import botocore
import logging
import json
import argparse
import awswrangler as wr
import tempfile
import time
import ast
from collections import Counter
from configparser import ConfigParser
from urllib.parse import urlparse, urlunparse

information_schema_name = "information_schema"
df_index = ['table_schema', 'table_name']


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

logger = logging.getLogger()

def get_config(s3_config_bucket,s3_config_file ):
    s3 = boto3.client('s3')
    body_content = s3.get_object(Bucket=s3_config_bucket, Key=s3_config_file)['Body'].read().decode('utf-8')
    config = ConfigParser()
    config.read_string(body_content)
    #config.read("glue_config.conf")
    return config


def get_client(region_name, service):
    session = boto3.Session(region_name=region_name)
    return session.client(service)


aws_region_list = ['us-east-2','us-east-1','us-west-1','us-west-2','af-south-1','ap-east-1','ap-south-1','ap-northeast-3'
    ,'ap-northeast-2','ap-southeast-1','ap-southeast-2','ap-northeast-1','ca-central-1','eu-central-1','eu-west-1','eu-west-2'
    ,'eu-south-1','eu-west-3','eu-north-1','me-south-1','sa-east-1']

def compare_df(source_df, target_df, df_index):
    source_df.set_index(df_index)
    target_df.set_index(df_index)
    merged_df = source_df.merge(target_df, how='outer', indicator=True)
    return merged_df[merged_df['_merge'] == 'both'], merged_df[merged_df['_merge'] == 'left_only'], merged_df[merged_df['_merge'] == 'right_only']

def get_database_name(row):
    resource_name = list(row.get('Resource', {}))
    if resource_name == ['Database']:
        return row.get('Resource', {}).get('Database', {}).get('Name', None)
    elif resource_name == ['Table']:
        return row.get('Resource', {}).get('Table', {}).get('DatabaseName', None)
    elif resource_name == ['TableWithColumns']:
        return row.get('Resource', {}).get('TableWithColumns', {}).get('DatabaseName', None)
    else:
        return None

def store_permission_data(permission_data,permissions_from_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name):
    with open("./lf_permissions.json", "w") as f:
        for pd in permission_data:
            f.write(json.dumps(pd) + "\n")
    output_file_name = f"s3://{lf_storage_bucket}/{lf_storage_folder}/{permissions_from_region}/{lf_storage_file_name}"
    print (f"Writing to output file name {output_file_name}")
    wr.s3.upload(local_file="lf_permissions.json", path=output_file_name)

def apply_table_permissions(file_location, destination_client, db_list):
    f = open(file_location, "r")
    for r_row in f.readlines():
        row  = json.loads(r_row)
        print (row)
        database_name = get_database_name(row)
        if database_name in db_list:
            print (f"Applying {row}")
            if 'Table' in row['Resource'] and 'Name' in row['Resource']['Table'] and row['Resource']['Table']['Name'] == 'ALL_TABLES':
                del row['Resource']['Table']['Name']
                row['Resource']['Table']['TableWildcard'] = {}
            if 'TableWithColumns' in row['Resource'] and 'Name' in row['Resource']['TableWithColumns'] and row['Resource']['TableWithColumns']['Name'] == 'ALL_TABLES':
                row['Resource']['Table'] = row['Resource']['TableWithColumns']
                del row['Resource']['Table']['Name']
                del row['Resource']['Table']['ColumnWildcard']
                del row['Resource']['TableWithColumns']
                row['Resource']['Table']['TableWildcard'] = {}
            response = destination_client.grant_permissions(**row)
            logger.info(response)


def get_permissions(source_client):
    logger.info("Processing permissions")
    result = source_client.list_permissions()
    principal_permissions = result['PrincipalResourcePermissions']
    fetch = True
    while fetch:
        try:
            token = result['NextToken']
            result = source_client.list_permissions(NextToken=token)
            principal_permissions.extend(result['PrincipalResourcePermissions'])
        except KeyError:
            fetch = False
    return principal_permissions


def create_table(glue_client, db_name, table):
    try:
        glue_client.create_table(DatabaseName=db_name, TableInput=table)
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=db_name, TableInput=table)

def create_database(glue_client,database_input):
    try:
        res = glue_client.create_database(DatabaseInput=database_input)
    except glue_client.exceptions.AlreadyExistsException:
        res = glue_client.update_database(DatabaseInput=database_input, Name=database_input['Name'])


def restore_data(config, data_source, glue_client,update_table_s3_location, table_s3_mapping):
    #glue_client = get_glue_client(config[data_source]['destination_region'])
    database_count = Counter()
    table_count = Counter()
    s3_path = config[data_source]['s3_data_path']
    f = tempfile.NamedTemporaryFile(mode='w',dir=".", delete=False)
    wr.s3.download(path=s3_path, local_file=f.name)
    with open(f.name, "r") as table_data_file:
        for table_data in table_data_file.readlines():
            logger.info(table_data)
            object_type, db_name, object_name, object_data = table_data.split("\t")
            #logger.info(object_type, db_name, object_name)
            if object_type == 'database':
                database_data = json.loads(object_data)
                logger.info(f"Restoring database {object_name} json data => {database_data}")
                database_s3_location_target = None
                if update_table_s3_location:
                    if 'LocationUri' in database_data:
                        database_s3_location = database_data.get('LocationUri')
                        if database_s3_location is not None:
                            u = urlparse(database_s3_location)
                            print (f"Received table_s3_mapping {table_s3_mapping}")
                            print (f"Received type table_s3_mapping {type(table_s3_mapping)}")
                            if u.netloc in table_s3_mapping:
                                target_s3_location = table_s3_mapping[u.netloc]
                                u = u._replace(netloc = target_s3_location)
                                print (u)
                                database_s3_location_target = urlunparse(u)
                            else:
                                database_s3_location_target = database_s3_location
                        database_data['LocationUri'] = database_s3_location_target
                create_database(glue_client, database_data)
                database_count[db_name] += 1
            elif object_type == 'table':
                table_data = json.loads(object_data)
                logger.info(f"Restoring table {object_name} json data => {table_data} ")
                table_s3_location_target = None
                if update_table_s3_location:
                    if 'Location' in database_data:
                        table_s3_location = database_data.get('Location', None)
                        if table_s3_location is not None:
                            u = urlparse(database_s3_location)
                            if u.netloc in table_s3_mapping:
                                target_s3_location = table_s3_mapping[u.netloc]
                                u = u._replace(netloc=target_s3_location)
                                table_s3_location_target = urlunparse(u)
                            else:
                                table_s3_location_target = table_s3_location
                        table_data['Location'] = table_s3_location_target
                create_table(glue_client, db_name, table_data)
                table_count[db_name] += 1

    for db_name in database_count.keys():
        logger.info(f"{db_name}=>table_count:{table_count[db_name]}")
    logger.info(
        f"Extracted database count => {len(list(database_count.keys()))}  table count => {len(list(table_count.elements()))}")

def get_tables(source_region, data_source, db_list):
    session_region = boto3.Session(region_name=source_region)
    db_list_string = "','".join(db_list)
    athena_query = f"""SELECT table_schema, table_name FROM  information_schema.tables
                       where table_schema in ('{db_list_string}') and table_catalog = lower('{data_source}')"""
    logger.info (f"Running query {athena_query}")
    df = wr.athena.read_sql_query(athena_query ,database=information_schema_name, ctas_approach=False, boto3_session=session_region)
    return df

def extract_database(source_region, output_file_name, db_list):
    table_count = Counter()
    database_count = Counter()
    glue_client = get_client(source_region,'glue')
    table_paginator = glue_client.get_paginator("get_tables")
    db_paginator = glue_client.get_paginator("get_databases")
    for page in db_paginator.paginate():
        with tempfile.NamedTemporaryFile(mode='w',dir=".", delete=False) as database_data_file:
            for db in page['DatabaseList']:
                if (db_list == ['ALL_DATABASE'] or (db['Name'] in db_list)):
                    logger.info (f"Database {db['Name']} matched with list of databases to be extracted")
                    col_to_be_removed = ['CreateTime', 'CatalogId','VersionId']
                    _db = [db.pop(key, '') for key in col_to_be_removed]
                    database_data_file.write(f"database\t{db['Name']}\t\t{json.dumps(db)}\n")
                    database_count[db['Name']] += 1
                    for page in table_paginator.paginate(DatabaseName=db['Name']):
                        for table in page['TableList']:
                            logger.info(f"Processing table {table['Name']}")
                            col_to_be_removed = ['CatalogId','DatabaseName','LastAccessTime', 'CreateTime', 'UpdateTime', 'CreatedBy','IsRegisteredWithLakeFormation','VersionId']
                            _table = [table.pop(key,'') for key in col_to_be_removed]
                            database_data_file.write(f"table\t{db['Name']}\t{table['Name']}\t{json.dumps(table)}\n")
                            table_count[db['Name']] += 1
        for db_name in table_count.keys():
            logger.info(f"{db_name}=>table_count:{table_count[db_name]}")
        logger.info(
            f"Extracted database count => {len(list(database_count.keys()))}  total table count => {len(list(table_count.elements()))}")
        wr.s3.upload(local_file=database_data_file.name, path=output_file_name)
        logger.info (f"Stored data in database_data_file.name {database_data_file.name}")
        logger.info (f"Output_file_name {output_file_name}")
    return database_count, table_count


def compare_db_tables(config, data_source):
    source_region = config[data_source]['source_region']
    destination_region = config[data_source]['destination_region']
    output_file_name = config[data_source]['s3_data_path']
    db_list = ast.literal_eval(config[data_source]['database_list'])
    start_time = time.time()
    logger.info(f"Starting processing at {time.asctime(time.localtime(time.time()))} with the following parameters ")
    logger.info(f"datasource => {data_source}")
    logger.info(f"source region name => {source_region}")
    logger.info(f"database => {db_list}")
    logger.info(f"output_file_name => {output_file_name}")
    logger.info("=============================================================================")
    source_tables_df = get_tables(source_region, data_source, db_list)
    destination_tables_df = get_tables(destination_region, data_source, db_list)
    matched_df, source_only_df, target_only_df = compare_df(source_tables_df, destination_tables_df, df_index)
    logger.info("=" * 50)
    if not matched_df.dropna().empty:
        logger.info("Matched Tables")
        logger.info(matched_df[df_index].to_string(index=False))
    else:
        logger.info ("No tables are matched")
    logger.info("-" * 50)
    if not source_only_df.dropna().empty:
        logger.info("Source only tables")
        logger.info(source_only_df[df_index].to_string(index=False)) if not source_only_df.empty else logger.info(
        "All tables copied, no tables in source left")
    else:
        logger.info ("No tables are found in the source")
    logger.info("-" * 50)
    if not target_only_df.dropna().empty:
        logger.info("Target only tables")
        logger.info(target_only_df[df_index].to_string(index=False)) if not target_only_df.empty else logger.info(
        "All tables copied, no addiitonal tables in  tables left")
    else:
        logger.info ("No tables are found in the target region")
    logger.info("-" * 50)

    return matched_df, source_only_df, target_only_df

def delete_target_tables(config, data_source):
    matched_df, source_only_df, target_only_df = compare_db_tables(config, data_source)
    tables_to_deleted = target_only_df
    #TODO  : delete tables

def main():
    global source_session
    global destination_session
    start_time = time.time()
    args = argparse.ArgumentParser()
    args.add_argument('--config-file-bucket',default=False,  dest='config_file_bucket', required=True, help="Config File bucket")
    args.add_argument('--config-file-key', default=False,  dest='config_file_key',required=True, help="Config File key")
    opts = args.parse_args()
    config_file_bucket = opts.config_file_bucket
    config_file_key = opts.config_file_key

    ## if there is / in the key name, this will remove it.
    config_file_key = config_file_key.lstrip("/") if config_file_key.startswith("/") else config_file_key
    logger.info (f"Reading config file from s3://{config_file_bucket}/{config_file_key}")
    config = get_config(config_file_bucket,config_file_key)
    delete_target_catalog_objects = config.getboolean('Operation', 'delete_target_catalog_objects')
    sync_glue_catalog = config.getboolean('Operation','sync_glue_catalog')
    sync_lf_permissions = config.getboolean('Operation', 'sync_lf_permissions')
    update_table_s3_location = config.getboolean('Target_s3_update','update_table_s3_location')
    table_s3_mapping = ast.literal_eval(config.get('AwsDataCatalog','target_s3_locations'))
    list_datasource = ast.literal_eval(config.get('ListCatalog','list_datasource'))

    logger.info (f"Received list of data sources {list_datasource}")
    source_lf_client = get_client(config['AwsDataCatalog']['source_region'],'lakeformation')
    destination_lf_client = get_client(config['AwsDataCatalog']['destination_region'],'lakeformation')
    glue_client = get_client(config['AwsDataCatalog']['destination_region'], 'glue')

    for data_source in list_datasource:
        source_region = config['AwsDataCatalog']['source_region']
        output_file_name = config[data_source]['s3_data_path']
        target_region = config['AwsDataCatalog']['destination_region']
        lf_storage_bucket = config['LakeFormationPermissions']['lf_storage_bucket']
        lf_storage_folder = config['LakeFormationPermissions']['lf_storage_file_folder']
        lf_storage_file_name = config['LakeFormationPermissions']['lf_storage_file_name']

        db_list = ast.literal_eval(config[data_source]['database_list'])

        if sync_glue_catalog:
            logger.info(f"Starting processing at {time.asctime(time.localtime(time.time()))} with the following parameters ")
            logger.info(f"datasource => {data_source}")
            logger.info(f"source region name => {source_region}")
            logger.info(f"database => {db_list}")
            logger.info(f"output_file_name => {output_file_name}")
            logger.info("=============================Starting Processing ================================================")
            database_count, table_count = extract_database(source_region, output_file_name, db_list)
            logger.info(f"Successfully extracted database count {database_count} and tables {table_count}")
            restore_data(config, data_source, glue_client,update_table_s3_location, table_s3_mapping)

        if delete_target_catalog_objects:
            delete_target_tables(config, data_source)


        if sync_lf_permissions:
            permission_data = get_permissions(source_lf_client)
            store_permission_data(permission_data,source_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name)
            permission_data = get_permissions(destination_lf_client)
            store_permission_data(permission_data,target_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name)
            print ("Reading permissions from s3 location")
            s3_client = get_client(source_region, 's3')
            with open(f"{lf_storage_file_name}", 'wb') as f:
                s3_client.download_fileobj(f"{lf_storage_bucket}", f"{lf_storage_folder}/{source_region}/{lf_storage_file_name}", f)
            apply_table_permissions(f"{config['LakeFormationPermissions']['lf_storage_file_name']}", destination_lf_client, db_list)
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Processing finished in {int(execution_time)} secs")
    logger.info("=" * 50)

if __name__ == '__main__':
    main()
