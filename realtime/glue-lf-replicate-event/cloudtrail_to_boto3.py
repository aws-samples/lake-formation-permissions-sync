key_alias = {
    'additionallocations': 'AdditionalLocations',
    'allowexternaldatafiltering' : 'AllowExternalDataFiltering',
    'allrowswildcard': 'AllRowsWildcard',
    'authorizedsessiontagvaluelist' : 'AuthorizedSessionTagValueList',
    'bucketcolumns': 'BucketColumns',
    'catalog': 'Catalog',
    'catalogid': 'CatalogId',
    'column': 'Column',
    'columnnames': 'ColumnNames',
    'columns': 'Columns',
    'columnwildcard': 'ColumnWildcard',
    'comment': 'Comment',
    'compressed': 'Compressed',
    'createdatabasedefaultpermissions' : 'CreateDatabaseDefaultPermissions',
    'createtabledefaultpermissions' : 'CreateTableDefaultPermissions',
    'database': 'Database',
    'databaseinput': 'DatabaseInput',
    'databasename': 'DatabaseName',
    'datacellsfilter': 'DataCellsFilter',
    'datalakeadmins' : 'DataLakeAdmins',
    'datalakeprincipalidentifier': 'DataLakePrincipalIdentifier',
    'datalakesettings': 'DataLakeSettings',
    'datalocation': 'DataLocation',
    'description': 'Description',
    'entries': 'Entries',
    'excludedcolumnnames': 'ExcludedColumnNames',
    'expression': 'Expression',
    'externaldatafilteringallowlist' : 'ExternalDataFilteringAllowList',
    'filterexpression': 'FilterExpression',
    'id': 'Id',
    'indexname': 'IndexName',
    'inputformat': 'InputFormat',
    'keys': 'Keys',
    'lastaccesstime': 'LastAccessTime',
    'lastanalyzedtime': 'LastAnalyzedTime',
    'lftag': 'LFTag',
    'lftagpolicy': 'LFTagPolicy',
    'lftags': 'LFTags',
    'location': 'Location',
    'locationuri': 'LocationUri',
    'name': 'Name',
    'numberofbuckets': 'NumberOfBuckets',
    'outputformat': 'OutputFormat',
    'owner': 'Owner',
    'parameters': 'Parameters',
    'partitionindexes':'PartitionIndexes',
    'partitioninput':'PartitionInput',
    'partitionkeys': 'PartitionKeys',
    'permissions': 'Permissions',
    'permissionswithgrantoption': 'PermissionsWithGrantOption',
    'principal': 'Principal',
    'registryname': 'RegistryName',
    'resource': 'Resource',
    'resourcearn': 'ResourceArn',
    'resourcetype': 'ResourceType',
    'retention': 'Retention',
    'rowfilter': 'RowFilter',
    'schemaarn': 'SchemaArn',
    'schemaid': 'SchemaId',
    'schemaname': 'SchemaName',
    'schemareference': 'SchemaReference',
    'schemaversionid': 'SchemaVersionId',
    'schemaversionnumber': 'SchemaVersionNumber',
    'serdeinfo': 'SerdeInfo',
    'serializationlibrary': 'SerializationLibrary',
    'skewedcolumnnames': 'SkewedColumnNames',
    'skewedcolumnvaluelocationmaps': 'SkewedColumnValueLocationMaps',
    'skewedcolumnvalues': 'SkewedColumnValues',
    'skewedinfo': 'SkewedInfo',
    'sortcolumns': 'SortColumns',
    'sortorder': 'SortOrder',
    'storagedescriptor': 'StorageDescriptor',
    'storedassubdirectories': 'StoredAsSubDirectories',
    'string': 'string',
    'table': 'Table',
    'tablecatalogid': 'TableCatalogId',
    'tabledata': 'TableData',
    'tableinput':'TableInput',
    'tablename': 'TableName',
    'tabletype': 'TableType',
    'tablewildcard': 'TableWildcard',
    'tablewithcolumns': 'TableWithColumns',
    'tagkey': 'TagKey',
    'tagvalues': 'TagValues',
    'targetdatabase': 'TargetDatabase',
    'targettable': 'TargetTable',
    'trustedresourceowners' : 'TrustedResourceOwners',
    'type': 'Type',
    'useservicelinkedrole':'UseServiceLinkedRole',
    'values': 'Values',
    'viewexpandedtext': 'ViewExpandedText',
    'partitioninputlist': 'PartitionInputList',
    'tagvaluestoadd': 'TagValuesToAdd',
    'tagvaluestodelete': 'TagValuesToDelete'
}


def parse_dict(obj):
    temp_dict = {}
    for k, v in obj.items():
        if isinstance(v, dict):
            temp_dict[key_alias.get(k.lower(), k)] = parse_dict(v)
        elif isinstance(v, list):
            if key_alias.get(k.lower(), k) not in temp_dict:
                temp_dict[key_alias.get(k.lower(), k)] = []
            temp_dict[key_alias.get(k.lower(), k)].extend(parse_list(v))
        else:
            temp_dict[key_alias.get(k.lower(), k)] = v
    return temp_dict

def parse_list(obj):
    temp_list = []
    for v in obj:
        if isinstance(v, dict):
            temp_list.append(parse_dict(v))
        elif isinstance(v, list):
            temp_list.append(parse_list(v))
        else:
            temp_list.append(key_alias.get(v.lower(), v))
    return temp_list

def cloudtail_to_boto3_converter(obj):
    """Recursively update cloudtrail request dict to boto3 request dict."""
    arr = {}
    for k, v in obj.items():
        if isinstance(v, dict):
            arr[key_alias.get(k.lower(), k)] = parse_dict(v)
        elif isinstance(v, list):
            if key_alias.get(k.lower(), k) not in arr:
                arr[key_alias.get(k.lower(), k)] = []
            arr[key_alias.get(k.lower(), k)].extend(parse_list(v))
        else:
            arr[key_alias.get(k.lower(), k)] = v
    return arr

