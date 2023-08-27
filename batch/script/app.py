import os
import sys
import boto3
import botocore
import json
import awswrangler as wr
import tempfile
import time
import ast
from collections import Counter
from configparser import ConfigParser
from urllib.parse import urlparse, urlunparse
from awsglue.utils import getResolvedOptions

information_schema_name = "information_schema"
df_index = ['table_schema', 'table_name']

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
    f = tempfile.TemporaryFile(mode='w+')
    for pd in permission_data:
        f.write(json.dumps(pd) + "\n")
    rf = open(f.name,'r+b')
    output_file_name = f"s3://{lf_storage_bucket}/{lf_storage_folder}/{permissions_from_region}/{lf_storage_file_name}"
    print (f"Writing to output file name {output_file_name}")
    wr.s3.upload(local_file=rf, path=output_file_name)
    f.close()

def apply_table_permissions(file_location, destination_client, db_list,source_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name):
    print ("Reading permissions from s3 location")
    f = tempfile.TemporaryFile(mode='w+b')
    s3_client = get_client(source_region, 's3')
    s3_client.download_fileobj(f"{lf_storage_bucket}", f"{lf_storage_folder}/{source_region}/{lf_storage_file_name}", f)
    print (f"{lf_storage_bucket}/{lf_storage_folder}/{source_region}/{lf_storage_file_name}")
    f.seek(0)
    rf = open(f.name,'r+')
    for r_row in rf.readlines():
        row  = json.loads(r_row)
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
    f.close()
    print ("Done applying table permissions")

def get_permissions(source_client):
    print("Processing permissions")
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

def update_location(s3_location, table_s3_mapping):
    if s3_location:
        u = urlparse(s3_location)
        if u.netloc in table_s3_mapping:
            target_s3_location = table_s3_mapping[u.netloc]
            u = u._replace(netloc=target_s3_location)
            return urlunparse(u)
    return s3_location

def update_database_location(database_data, table_s3_mapping):
    if 'LocationUri' in database_data:
        database_data['LocationUri'] = update_location(database_data['LocationUri'], table_s3_mapping)
    return database_data

def update_table_location(table_data, table_s3_mapping):
    storage_descriptor = table_data.get('StorageDescriptor', {})
    if 'Location' in storage_descriptor:
        table_data['StorageDescriptor']['Location'] = update_location(storage_descriptor.get('Location'), table_s3_mapping)
    return table_data

def create_or_update_partition(glue_client, db_name, table_name, partition_data, update_table_s3_location, table_s3_mapping):
    try:
        if update_table_s3_location:
            partition_data = update_table_location(partition_data, table_s3_mapping)
        partition_input = {
            'Values': partition_data.get('Values', []),
            'StorageDescriptor': partition_data.get('StorageDescriptor', {}),
            'Parameters': partition_data.get('Parameters', {})
        }
        glue_client.create_partition(
            DatabaseName=db_name,
            TableName=table_name,
            PartitionInput=partition_input
        )
        print(f"Successfully created partition: {partition_data['Values']}")
    except glue_client.exceptions.AlreadyExistsException:
        try:
            glue_client.update_partition(
                DatabaseName=db_name,
                TableName=table_name,
                PartitionValueList=partition_data['Values'],
                PartitionInput=partition_input
            )
            print(f"Successfully updated partition: {partition_data['Values']}")
        except Exception as update_err:
            print(f"Failed to update partition {partition_data['Values']}. Reason: {update_err}")
            raise update_err
    except Exception as e:
        print(f"Failed to create partition {partition_data['Values']}. Reason: {e}")
        raise e

def restore_data(config, data_source, glue_client, update_table_s3_location, table_s3_mapping):
    print("Restoring database...")
    database_count = Counter()
    table_count = Counter()
    partition_count = Counter()
    s3_path = config[data_source]['s3_data_path']
    f = tempfile.TemporaryFile(mode='w+b')
    wr.s3.download(path=s3_path, local_file=f)
    f.seek(0)
    rf = open(f.name, "r+t")
    for object_data_line in rf.readlines():
        object_type, db_name, object_name, object_data = object_data_line.split("\t")
        print(f"Processing object_type {object_type} {db_name} {object_name} ")
        if object_type == 'database':
            database_data = json.loads(object_data)
            if update_table_s3_location:
                database_data = update_database_location(database_data, table_s3_mapping)
            create_database(glue_client, database_data)
            database_count[db_name] += 1
        elif object_type == 'table':
            table_data = json.loads(object_data)
            if update_table_s3_location:
                table_data = update_table_location(table_data, table_s3_mapping)
            create_table(glue_client, db_name, table_data)
            table_count[db_name] += 1
        elif object_type == 'partition':
            partition_data = json.loads(object_data)
            create_or_update_partition(glue_client, db_name, object_name, partition_data, update_table_s3_location, table_s3_mapping)
            partition_count[db_name] += 1
    rf.close()
    for db_name in database_count.keys():
        print(f"{db_name}=>table_count:{table_count[db_name]} partition_count:{partition_count[db_name]}")
    print(f"Restored database count => {len(list(database_count.keys()))}  table count => {len(list(table_count.elements()))}  partition count => {len(list(partition_count.elements()))}")

def get_tables(source_region, data_source, db_list):
    session_region = boto3.Session(region_name=source_region)
    db_list_string = "','".join(db_list)
    athena_query = f"""SELECT table_schema, table_name FROM  information_schema.tables
                       where table_schema in ('{db_list_string}') and table_catalog = lower('{data_source}')"""
    print (f"Running query {athena_query}")
    df = wr.athena.read_sql_query(athena_query ,database=information_schema_name, ctas_approach=False, boto3_session=session_region)
    return df

def extract_database(source_region, output_file_name, db_list):
    print ("Extracting database...")
    table_count = Counter()
    database_count = Counter()
    partition_count = Counter()
    glue_client = get_client(source_region,'glue')
    table_paginator = glue_client.get_paginator("get_tables")
    partition_paginator = glue_client.get_paginator("get_partitions")
    db_paginator = glue_client.get_paginator("get_databases")
    for page in db_paginator.paginate():
        database_data_file = tempfile.TemporaryFile(mode='w+')
        for db in page['DatabaseList']:
            if (db_list == ['ALL_DATABASE'] or (db['Name'] in db_list)):
                print (f"Database {db['Name']} matched with list of databases to be extracted")
                col_to_be_removed = ['CreateTime', 'CatalogId','VersionId']
                _db = [db.pop(key, '') for key in col_to_be_removed]
                database_data_file.write(f"database\t{db['Name']}\t\t{json.dumps(db)}\n")
                database_count[db['Name']] += 1
                for page in table_paginator.paginate(DatabaseName=db['Name']):
                    for table in page['TableList']:
                        print(f"Processing table {table['Name']}")
                        col_to_be_removed = ['CatalogId','DatabaseName','LastAccessTime', 'CreateTime', 'UpdateTime', 'CreatedBy','IsRegisteredWithLakeFormation','VersionId']
                        _table = [table.pop(key,'') for key in col_to_be_removed]
                        database_data_file.write(f"table\t{db['Name']}\t{table['Name']}\t{json.dumps(table)}\n")
                        table_count[db['Name']] += 1
                        for partition_page in partition_paginator.paginate(DatabaseName=db['Name'], TableName=table['Name']):
                            for partition in partition_page['Partitions']:
                                print(f"Processing partition {partition['Values']} for table {table['Name']}")
                                col_to_be_removed = ['CatalogId', 'DatabaseName', 'CreationTime', 'LastAccessTime']
                                _partition = [partition.pop(key,'') for key in col_to_be_removed]
                                database_data_file.write(f"partition\t{db['Name']}\t{table['Name']}\t{json.dumps(partition)}\n")
                                partition_count[db['Name']] += 1
        for db_name in table_count.keys():
            print(f"{db_name}=>table_count:{table_count[db_name]}")
        database_data_file.seek(0)
        print (f"database_data_file.name => {database_data_file.name}")
        with open(database_data_file.name, 'rb') as rf:
            wr.s3.upload(local_file=rf, path=output_file_name)
            print(f"Stored data in database_data_file.name {database_data_file.name}")
            print(f"Output_file_name {output_file_name}")
    print(f"Extracted database count => {len(list(database_count.keys()))}  total table count => {len(list(table_count.elements()))}")


def compare_db_tables(config, data_source):
    source_region = config[data_source]['source_region']
    destination_region = config[data_source]['destination_region']
    output_file_name = config[data_source]['s3_data_path']
    db_list = ast.literal_eval(config[data_source]['database_list'])
    start_time = time.time()
    print(f"Starting processing at {time.asctime(time.localtime(time.time()))} with the following parameters ")
    print(f"datasource => {data_source}")
    print(f"source region name => {source_region}")
    print(f"database => {db_list}")
    print(f"output_file_name => {output_file_name}")
    print("=============================================================================")
    source_tables_df = get_tables(source_region, data_source, db_list)
    destination_tables_df = get_tables(destination_region, data_source, db_list)
    matched_df, source_only_df, target_only_df = compare_df(source_tables_df, destination_tables_df, df_index)
    print("=" * 50)
    if not matched_df.dropna().empty:
        print("Matched Tables")
        print(matched_df[df_index].to_string(index=False))
    else:
        print ("No tables are matched")
    print("-" * 50)
    if not source_only_df.dropna().empty:
        print("Source only tables")
        print(source_only_df[df_index].to_string(index=False)) if not source_only_df.empty else print(
        "All tables copied, no tables in source left")
    else:
        print ("No tables are found in the source")
    print("-" * 50)
    if not target_only_df.dropna().empty:
        print("Target only tables")
        print(target_only_df[df_index].to_string(index=False)) if not target_only_df.empty else print(
        "All tables copied, no addiitonal tables in  tables left")
    else:
        print ("No tables are found in the target region")
    print("-" * 50)

    return matched_df, source_only_df, target_only_df

def delete_target_tables(config, data_source):
    matched_df, source_only_df, target_only_df = compare_db_tables(config, data_source)
    tables_to_deleted = target_only_df
    #TODO  : delete tables

def main():
    global source_session
    global destination_session
    start_time = time.time()

    args = getResolvedOptions(sys.argv, ['CONFIG_BUCKET','CONFIG_FILE_KEY'])

    config_file_bucket = args['CONFIG_BUCKET']
    config_file_key = args['CONFIG_FILE_KEY']

    ## if there is / in the key name, this will remove it.
    config_file_key = config_file_key.lstrip("/") if config_file_key.startswith("/") else config_file_key
    print (f"Reading config file from s3://{config_file_bucket}/{config_file_key}")
    config = get_config(config_file_bucket,config_file_key)
    delete_target_catalog_objects = config.getboolean('Operation', 'delete_target_catalog_objects')
    sync_glue_catalog = config.getboolean('Operation','sync_glue_catalog')
    sync_lf_permissions = config.getboolean('Operation', 'sync_lf_permissions')
    update_table_s3_location = config.getboolean('Target_s3_update','update_table_s3_location')
    table_s3_mapping = ast.literal_eval(config.get('AwsDataCatalog','target_s3_locations'))
    list_datasource = ast.literal_eval(config.get('ListCatalog','list_datasource'))

    print(f"Received list of data sources {list_datasource}")
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
            print(f"Starting processing at {time.asctime(time.localtime(time.time()))} with the following parameters ")
            print(f"datasource => {data_source}")
            print(f"source region name => {source_region}")
            print(f"database => {db_list}")
            print(f"output_file_name => {output_file_name}")
            print("=============================Starting Processing ================================================")
            extract_database(source_region, output_file_name, db_list)
            restore_data(config, data_source, glue_client,update_table_s3_location, table_s3_mapping)

        if delete_target_catalog_objects:
            delete_target_tables(config, data_source)


        if sync_lf_permissions:
            permission_data = get_permissions(source_lf_client)
            store_permission_data(permission_data,source_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name)
            permission_data = get_permissions(destination_lf_client)
            store_permission_data(permission_data,target_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name)
            apply_table_permissions(f"{config['LakeFormationPermissions']['lf_storage_file_name']}", destination_lf_client, db_list,source_region,lf_storage_bucket,lf_storage_folder,lf_storage_file_name)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Processing finished in {int(execution_time)} secs")
    print("=" * 50)

main()