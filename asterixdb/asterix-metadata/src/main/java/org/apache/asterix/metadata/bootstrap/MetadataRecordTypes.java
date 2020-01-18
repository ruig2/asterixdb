/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.metadata.bootstrap;

import static org.apache.asterix.om.types.BuiltinType.ASTRING;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * Contains static ARecordType's of all metadata record types.
 */
public final class MetadataRecordTypes {

    //--------------------------------------- Fields Names --------------------------------------//
    public static final String FIELD_NAME_ADAPTER_CONFIGURATION = "AdapterConfiguration";
    public static final String FIELD_NAME_ADAPTER_NAME = "AdapterName";
    public static final String FIELD_NAME_ARITY = "Arity";
    public static final String FIELD_NAME_AUTOGENERATED = "Autogenerated";
    public static final String FIELD_NAME_CLASSNAME = "Classname";
    public static final String FIELD_NAME_COMPACTION_POLICY = "CompactionPolicy";
    public static final String FIELD_NAME_COMPACTION_POLICY_PROPERTIES = "CompactionPolicyProperties";
    public static final String FIELD_NAME_DATASET_ID = "DatasetId";
    public static final String FIELD_NAME_DATASET_NAME = "DatasetName";
    public static final String FIELD_NAME_DATASET_TYPE = "DatasetType";
    public static final String FIELD_NAME_DATASOURCE_ADAPTER = "DatasourceAdapter";
    public static final String FIELD_NAME_DATATYPE_DATAVERSE_NAME = "DatatypeDataverseName";
    public static final String FIELD_NAME_DATATYPE_NAME = "DatatypeName";
    public static final String FIELD_NAME_DATAVERSE_NAME = "DataverseName";
    public static final String FIELD_NAME_DATA_FORMAT = "DataFormat";
    public static final String FIELD_NAME_DEFINITION = "Definition";
    public static final String FIELD_NAME_DEPENDENCIES = "Dependencies";
    public static final String FIELD_NAME_DERIVED = "Derived";
    public static final String FIELD_NAME_DESCRIPTION = "Description";
    public static final String FIELD_NAME_EXTERNAL_DETAILS = "ExternalDetails";
    public static final String FIELD_NAME_FEED_NAME = "FeedName";
    public static final String FIELD_NAME_FEED_TYPE = "FeedType";
    public static final String FIELD_NAME_FIELDS = "Fields";
    public static final String FIELD_NAME_FIELD_NAME = "FieldName";
    public static final String FIELD_NAME_FIELD_TYPE = "FieldType";
    public static final String FIELD_NAME_FILE_MOD_TIME = "FileModTime";
    public static final String FIELD_NAME_FILE_NAME = "FileName";
    public static final String FIELD_NAME_FILE_NUMBER = "FileNumber";
    public static final String FIELD_NAME_FILE_SIZE = "FileSize";
    public static final String FIELD_NAME_FILE_STRUCTURE = "FileStructure";
    public static final String FIELD_NAME_GROUP_NAME = "GroupName";
    public static final String FIELD_NAME_HINTS = "Hints";
    public static final String FIELD_NAME_INDEX_NAME = "IndexName";
    public static final String FIELD_NAME_INDEX_STRUCTURE = "IndexStructure";
    public static final String FIELD_NAME_INTERNAL_DETAILS = "InternalDetails";
    public static final String FIELD_NAME_IS_ANONYMOUS = "IsAnonymous";
    public static final String FIELD_NAME_IS_NULLABLE = "IsNullable";
    public static final String FIELD_NAME_IS_OPEN = "IsOpen";
    public static final String FIELD_NAME_IS_PRIMARY = "IsPrimary";
    public static final String FIELD_NAME_KIND = "Kind";
    public static final String FIELD_NAME_LANGUAGE = "Language";
    public static final String FIELD_NAME_LAST_REFRESH_TIME = "LastRefreshTime";
    public static final String FIELD_NAME_METATYPE_DATAVERSE_NAME = "MetatypeDataverseName";
    public static final String FIELD_NAME_METATYPE_NAME = "MetatypeName";
    public static final String FIELD_NAME_NAME = "Name";
    public static final String FIELD_NAME_NODE_NAME = "NodeName";
    public static final String FIELD_NAME_NODE_NAMES = "NodeNames";
    public static final String FIELD_NAME_NUMBER_OF_CORES = "NumberOfCores";
    public static final String FIELD_NAME_OBJECT_DATAVERSE_NAME = "ObjectDataverseName";
    public static final String FIELD_NAME_OBJECT_NAME = "ObjectName";
    public static final String FIELD_NAME_ORDERED_LIST = "OrderedList";
    public static final String FIELD_NAME_PARAMS = "Params";
    public static final String FIELD_NAME_PARTITIONING_KEY = "PartitioningKey";
    public static final String FIELD_NAME_PARTITIONING_STRATEGY = "PartitioningStrategy";
    public static final String FIELD_NAME_PENDING_OP = "PendingOp";
    public static final String FIELD_NAME_POLICY_NAME = "PolicyName";
    public static final String FIELD_NAME_PRIMARY_KEY = "PrimaryKey";
    public static final String FIELD_NAME_PROPERTIES = "Properties";
    public static final String FIELD_NAME_RECORD = "Record";
    public static final String FIELD_NAME_RETURN_TYPE = "ReturnType";
    public static final String FIELD_NAME_SEARCH_KEY = "SearchKey";
    public static final String FIELD_NAME_STATUS = "Status";
    public static final String FIELD_NAME_SYNONYM_NAME = "SynonymName";
    public static final String FIELD_NAME_TAG = "Tag";
    public static final String FIELD_NAME_TIMESTAMP = "Timestamp";
    public static final String FIELD_NAME_TRANSACTION_STATE = "TransactionState";
    public static final String FIELD_NAME_TYPE = "Type";
    public static final String FIELD_NAME_UNORDERED_LIST = "UnorderedList";
    public static final String FIELD_NAME_VALUE = "Value";
    public static final String FIELD_NAME_WORKING_MEMORY_SIZE = "WorkingMemorySize";
    public static final String FIELD_NAME_APPLIED_FUNCTIONS = "AppliedFunctions";
    public static final String FIELD_NAME_WHERE_CLAUSE = "WhereClause";
    public static final String FIELD_NAME_FULLTEXT_CATEGORY = "FulltextEntityCategory";
    public static final String FIELD_NAME_FULLTEXT_ENTITY_NAME = "FulltextEntityName";
    public static final String FIELD_NAME_FULLTEXT_FILTER_CATEGORY = "FulltextFilterType";
    public static final String FIELD_NAME_FULLTEXT_TOKENIZER = "Tokenizer";
    public static final String FIELD_NAME_FULLTEXT_FILTER_PIPELINE = "FilterPipeline";
    public static final String FIELD_NAME_FULLTEXT_USED_BY_CONFIGS = "UsedByConfigs";
    public static final String FIELD_NAME_FULLTEXT_USED_BY_INDICES = "UsedByIndices";
    public static final String FIELD_NAME_FULLTEXT_STOPWORD_LIST = "StopwordList";

    //---------------------------------- Record Types Creation ----------------------------------//
    //--------------------------------------- Properties ----------------------------------------//
    public static final int PROPERTIES_NAME_FIELD_INDEX = 0;
    public static final int PROPERTIES_VALUE_FIELD_INDEX = 1;
    public static final ARecordType POLICY_PARAMS_RECORDTYPE = createPropertiesRecordType();
    public static final ARecordType DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE = createPropertiesRecordType();
    public static final ARecordType COMPACTION_POLICY_PROPERTIES_RECORDTYPE = createPropertiesRecordType();
    public static final ARecordType DATASET_HINTS_RECORDTYPE = createPropertiesRecordType();
    public static final ARecordType FEED_ADAPTER_CONFIGURATION_RECORDTYPE = createPropertiesRecordType();

    //----------------------------- Internal Details Record Type --------------------------------//
    public static final int INTERNAL_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX = 0;
    public static final int INTERNAL_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX = 1;
    public static final int INTERNAL_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX = 2;
    public static final int INTERNAL_DETAILS_ARECORD_PRIMARYKEY_FIELD_INDEX = 3;
    public static final int INTERNAL_DETAILS_ARECORD_AUTOGENERATED_FIELD_INDEX = 4;
    public static final ARecordType INTERNAL_DETAILS_RECORDTYPE = createRecordType(
            // RecordTypeName
            null,
            // FieldNames
            new String[] { FIELD_NAME_FILE_STRUCTURE, FIELD_NAME_PARTITIONING_STRATEGY, FIELD_NAME_PARTITIONING_KEY,
                    FIELD_NAME_PRIMARY_KEY, FIELD_NAME_AUTOGENERATED },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, new AOrderedListType(new AOrderedListType(ASTRING, null), null),
                    new AOrderedListType(new AOrderedListType(ASTRING, null), null), BuiltinType.ABOOLEAN },
            //IsOpen?
            true);

    //----------------------------- External Details Record Type --------------------------------//
    public static final int EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX = 0;
    public static final int EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX = 1;
    public static final int EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX = 2;
    public static final int EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX = 3;
    public static final ARecordType EXTERNAL_DETAILS_RECORDTYPE = createRecordType(
            // RecordTypeName
            null,
            // FieldNames
            new String[] { FIELD_NAME_DATASOURCE_ADAPTER, FIELD_NAME_PROPERTIES, FIELD_NAME_LAST_REFRESH_TIME,
                    FIELD_NAME_TRANSACTION_STATE },
            // FieldTypes
            new IAType[] { ASTRING, new AOrderedListType(DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE, null),
                    BuiltinType.ADATETIME, BuiltinType.AINT32 },
            //IsOpen?
            true);
    //---------------------------------------- Dataset ------------------------------------------//
    public static final String RECORD_NAME_DATASET = "DatasetRecordType";
    public static final int DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATASET_ARECORD_DATASETNAME_FIELD_INDEX = 1;
    public static final int DATASET_ARECORD_DATATYPEDATAVERSENAME_FIELD_INDEX = 2;
    public static final int DATASET_ARECORD_DATATYPENAME_FIELD_INDEX = 3;
    public static final int DATASET_ARECORD_DATASETTYPE_FIELD_INDEX = 4;
    public static final int DATASET_ARECORD_GROUPNAME_FIELD_INDEX = 5;
    public static final int DATASET_ARECORD_COMPACTION_POLICY_FIELD_INDEX = 6;
    public static final int DATASET_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX = 7;
    public static final int DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX = 8;
    public static final int DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX = 9;
    public static final int DATASET_ARECORD_HINTS_FIELD_INDEX = 10;
    public static final int DATASET_ARECORD_TIMESTAMP_FIELD_INDEX = 11;
    public static final int DATASET_ARECORD_DATASETID_FIELD_INDEX = 12;
    public static final int DATASET_ARECORD_PENDINGOP_FIELD_INDEX = 13;
    //Optional open fields
    public static final String DATASET_ARECORD_BLOCK_LEVEL_STORAGE_COMPRESSION_FIELD_NAME =
            "BlockLevelStorageCompression";
    public static final String DATASET_ARECORD_DATASET_COMPRESSION_SCHEME_FIELD_NAME = "DatasetCompressionScheme";
    public static final String DATASET_ARECORD_REBALANCE_FIELD_NAME = "rebalanceCount";
    public static final ARecordType DATASET_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_DATASET,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_DATATYPE_DATAVERSE_NAME,
                    FIELD_NAME_DATATYPE_NAME, FIELD_NAME_DATASET_TYPE, FIELD_NAME_GROUP_NAME,
                    FIELD_NAME_COMPACTION_POLICY, FIELD_NAME_COMPACTION_POLICY_PROPERTIES, FIELD_NAME_INTERNAL_DETAILS,
                    FIELD_NAME_EXTERNAL_DETAILS, FIELD_NAME_HINTS, FIELD_NAME_TIMESTAMP, FIELD_NAME_DATASET_ID,
                    FIELD_NAME_PENDING_OP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, ASTRING, ASTRING, ASTRING, ASTRING,
                    new AOrderedListType(COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null),
                    AUnionType.createUnknownableType(INTERNAL_DETAILS_RECORDTYPE),
                    AUnionType.createUnknownableType(EXTERNAL_DETAILS_RECORDTYPE),
                    new AUnorderedListType(DATASET_HINTS_RECORDTYPE, null), ASTRING, BuiltinType.AINT32,
                    BuiltinType.AINT32 },
            //IsOpen?
            true);

    //------------------------------------------ Field ------------------------------------------//
    public static final int FIELD_ARECORD_FIELDNAME_FIELD_INDEX = 0;
    public static final int FIELD_ARECORD_FIELDTYPE_FIELD_INDEX = 1;
    public static final int FIELD_ARECORD_ISNULLABLE_FIELD_INDEX = 2;
    public static final ARecordType FIELD_RECORDTYPE = createRecordType(
            // RecordTypeName
            null,
            // FieldNames
            new String[] { FIELD_NAME_FIELD_NAME, FIELD_NAME_FIELD_TYPE, FIELD_NAME_IS_NULLABLE },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, BuiltinType.ABOOLEAN },
            //IsOpen?
            true);
    //---------------------------------------- Record Type --------------------------------------//
    public static final int RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX = 0;
    public static final int RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX = 1;
    public static final ARecordType RECORD_RECORDTYPE = createRecordType(
            // RecordTypeName
            null,
            // FieldNames
            new String[] { FIELD_NAME_IS_OPEN, FIELD_NAME_FIELDS },
            // FieldTypes
            new IAType[] { BuiltinType.ABOOLEAN, new AOrderedListType(FIELD_RECORDTYPE, null) },
            //IsOpen?
            true);

    //-------------------------------------- Derived Type ---------------------------------------//
    public static final int DERIVEDTYPE_ARECORD_TAG_FIELD_INDEX = 0;
    public static final int DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX = 1;
    public static final int DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX = 2;
    public static final int DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX = 3;
    public static final int DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX = 4;
    public static final ARecordType DERIVEDTYPE_RECORDTYPE = createRecordType(
            // RecordTypeName
            null,
            // FieldNames
            new String[] { FIELD_NAME_TAG, FIELD_NAME_IS_ANONYMOUS, FIELD_NAME_RECORD, FIELD_NAME_UNORDERED_LIST,
                    FIELD_NAME_ORDERED_LIST },
            // FieldTypes
            new IAType[] { ASTRING, BuiltinType.ABOOLEAN, AUnionType.createUnknownableType(RECORD_RECORDTYPE),
                    AUnionType.createUnknownableType(ASTRING), AUnionType.createUnknownableType(ASTRING) },
            //IsOpen?
            true);
    //---------------------------------------- Data Type ----------------------------------------//
    public static final String RECORD_NAME_DATATYPE = "DatatypeRecordType";
    public static final int DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX = 1;
    public static final int DATATYPE_ARECORD_DERIVED_FIELD_INDEX = 2;
    public static final int DATATYPE_ARECORD_TIMESTAMP_FIELD_INDEX = 3;
    public static final ARecordType DATATYPE_RECORDTYPE =
            createRecordType(
                    // RecordTypeName
                    RECORD_NAME_DATATYPE,
                    // FieldNames
                    new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATATYPE_NAME, FIELD_NAME_DERIVED,
                            FIELD_NAME_TIMESTAMP },
                    // FieldTypes
                    new IAType[] { ASTRING, ASTRING, AUnionType.createUnknownableType(DERIVEDTYPE_RECORDTYPE),
                            ASTRING },
                    //IsOpen?
                    true);
    //-------------------------------------- Dataverse ------------------------------------------//
    public static final String RECORD_NAME_DATAVERSE = "DataverseRecordType";
    public static final int DATAVERSE_ARECORD_NAME_FIELD_INDEX = 0;
    public static final int DATAVERSE_ARECORD_FORMAT_FIELD_INDEX = 1;
    public static final int DATAVERSE_ARECORD_TIMESTAMP_FIELD_INDEX = 2;
    public static final int DATAVERSE_ARECORD_PENDINGOP_FIELD_INDEX = 3;
    public static final ARecordType DATAVERSE_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_DATAVERSE,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATA_FORMAT, FIELD_NAME_TIMESTAMP,
                    FIELD_NAME_PENDING_OP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, BuiltinType.AINT32 },
            //IsOpen?
            true);
    //-------------------------------------------- Index ----------------------------------------//
    public static final String RECORD_NAME_INDEX = "IndexRecordType";
    public static final int INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int INDEX_ARECORD_DATASETNAME_FIELD_INDEX = 1;
    public static final int INDEX_ARECORD_INDEXNAME_FIELD_INDEX = 2;
    public static final int INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX = 3;
    public static final int INDEX_ARECORD_SEARCHKEY_FIELD_INDEX = 4;
    public static final int INDEX_ARECORD_ISPRIMARY_FIELD_INDEX = 5;
    public static final int INDEX_ARECORD_TIMESTAMP_FIELD_INDEX = 6;
    public static final int INDEX_ARECORD_PENDINGOP_FIELD_INDEX = 7;
    public static final ARecordType INDEX_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_INDEX,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_INDEX_NAME,
                    FIELD_NAME_INDEX_STRUCTURE, FIELD_NAME_SEARCH_KEY, FIELD_NAME_IS_PRIMARY, FIELD_NAME_TIMESTAMP,
                    FIELD_NAME_PENDING_OP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, ASTRING,
                    new AOrderedListType(new AOrderedListType(ASTRING, null), null), BuiltinType.ABOOLEAN, ASTRING,
                    BuiltinType.AINT32 },
            //IsOpen?
            true);
    //----------------------------------------- Node --------------------------------------------//
    public static final String RECORD_NAME_NODE = "NodeRecordType";
    public static final int NODE_ARECORD_NODENAME_FIELD_INDEX = 0;
    public static final int NODE_ARECORD_NUMBEROFCORES_FIELD_INDEX = 1;
    public static final int NODE_ARECORD_WORKINGMEMORYSIZE_FIELD_INDEX = 2;
    public static final ARecordType NODE_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_NODE,
            // FieldNames
            new String[] { FIELD_NAME_NODE_NAME, FIELD_NAME_NUMBER_OF_CORES, FIELD_NAME_WORKING_MEMORY_SIZE },
            // FieldTypes
            new IAType[] { ASTRING, BuiltinType.AINT64, BuiltinType.AINT64 },
            //IsOpen?
            true);
    //--------------------------------------- Node Group ----------------------------------------//
    public static final String RECORD_NAME_NODE_GROUP = "NodeGroupRecordType";
    public static final int NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX = 0;
    public static final int NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX = 1;
    public static final int NODEGROUP_ARECORD_TIMESTAMP_FIELD_INDEX = 2;
    public static final ARecordType NODEGROUP_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_NODE_GROUP,
            // FieldNames
            new String[] { FIELD_NAME_GROUP_NAME, FIELD_NAME_NODE_NAMES, FIELD_NAME_TIMESTAMP },
            // FieldTypes
            new IAType[] { ASTRING, new AUnorderedListType(ASTRING, null), ASTRING },
            //IsOpen?
            true);
    //----------------------------------------- Function ----------------------------------------//
    public static final String RECORD_NAME_FUNCTION = "FunctionRecordType";
    public static final int FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX = 1;
    public static final int FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX = 2;
    public static final int FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX = 3;
    public static final int FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX = 4;
    public static final int FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX = 5;
    public static final int FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX = 6;
    public static final int FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX = 7;
    public static final int FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX = 8;
    public static final ARecordType FUNCTION_RECORDTYPE =
            createRecordType(
                    // RecordTypeName
                    RECORD_NAME_FUNCTION,
                    // FieldNames
                    new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_ARITY, FIELD_NAME_PARAMS,
                            FIELD_NAME_RETURN_TYPE, FIELD_NAME_DEFINITION, FIELD_NAME_LANGUAGE, FIELD_NAME_KIND,
                            FIELD_NAME_DEPENDENCIES },
                    // FieldTypes
                    new IAType[] { ASTRING, ASTRING, ASTRING, new AOrderedListType(ASTRING, null), ASTRING, ASTRING,
                            ASTRING, ASTRING,
                            new AOrderedListType(new AOrderedListType(new AOrderedListType(ASTRING, null), null),
                                    null) },
                    //IsOpen?
                    true);
    //------------------------------------------ Adapter ----------------------------------------//
    public static final String RECORD_NAME_DATASOURCE_ADAPTER = "DatasourceAdapterRecordType";
    public static final int DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX = 1;
    public static final int DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX = 2;
    public static final int DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX = 3;
    public static final int DATASOURCE_ADAPTER_ARECORD_TIMESTAMP_FIELD_INDEX = 4;
    public static final ARecordType DATASOURCE_ADAPTER_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_DATASOURCE_ADAPTER,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_CLASSNAME, FIELD_NAME_TYPE,
                    FIELD_NAME_TIMESTAMP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, ASTRING, ASTRING },
            //IsOpen?
            true);

    //---------------------------------------- Feed Details ------------------------------------//
    public static final String RECORD_NAME_FEED = "FeedRecordType";
    public static final int FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_ARECORD_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_ARECORD_ADAPTOR_CONFIG_INDEX = 2;
    public static final int FEED_ARECORD_TIMESTAMP_FIELD_INDEX = 3;
    public static final ARecordType FEED_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_FEED,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FEED_NAME, FIELD_NAME_ADAPTER_CONFIGURATION,
                    FIELD_NAME_TIMESTAMP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, new AUnorderedListType(FEED_ADAPTER_CONFIGURATION_RECORDTYPE, null),
                    ASTRING },
            //IsOpen?
            true);

    //------------------------------------- Feed Connection ---------------------------------------//
    public static final String RECORD_NAME_FEED_CONNECTION = "FeedConnectionRecordType";
    public static final int FEED_CONN_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_CONN_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_CONN_DATASET_NAME_FIELD_INDEX = 2;
    public static final int FEED_CONN_OUTPUT_TYPE_INDEX = 3;
    public static final int FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX = 4;
    public static final int FEED_CONN_POLICY_FIELD_INDEX = 5;

    public static final ARecordType FEED_CONNECTION_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_FEED_CONNECTION,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_FEED_NAME, FIELD_NAME_DATASET_NAME,
                    FIELD_NAME_RETURN_TYPE, FIELD_NAME_APPLIED_FUNCTIONS, FIELD_NAME_POLICY_NAME },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, ASTRING, new AUnorderedListType(ASTRING, null), ASTRING },
            //IsOpen?
            true);

    //------------------------------------- Feed Policy ---------------------------------------//
    public static final String RECORD_NAME_FEED_POLICY = "FeedPolicyRecordType";
    public static final int FEED_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX = 1;
    public static final int FEED_POLICY_ARECORD_DESCRIPTION_FIELD_INDEX = 2;
    public static final int FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX = 3;
    public static final ARecordType FEED_POLICY_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_FEED_POLICY,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_POLICY_NAME, FIELD_NAME_DESCRIPTION,
                    FIELD_NAME_PROPERTIES },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING, new AUnorderedListType(POLICY_PARAMS_RECORDTYPE, null) },
            //IsOpen?
            true);
    //---------------------------------------- Library ------------------------------------------//
    public static final String RECORD_NAME_LIBRARY = "LibraryRecordType";
    public static final int LIBRARY_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int LIBRARY_ARECORD_NAME_FIELD_INDEX = 1;
    public static final int LIBRARY_ARECORD_TIMESTAMP_FIELD_INDEX = 2;
    public static final ARecordType LIBRARY_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_LIBRARY,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_NAME, FIELD_NAME_TIMESTAMP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING },
            //IsOpen?
            true);
    //------------------------------------- Compaction Policy -----------------------------------//
    public static final String RECORD_NAME_COMPACTION_POLICY = "CompactionPolicyRecordType";
    public static final int COMPACTION_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int COMPACTION_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX = 1;
    public static final int COMPACTION_POLICY_ARECORD_CLASSNAME_FIELD_INDEX = 2;
    public static final ARecordType COMPACTION_POLICY_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_COMPACTION_POLICY,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_COMPACTION_POLICY, FIELD_NAME_CLASSNAME },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, ASTRING },
            //IsOpen?
            true);
    //-------------------------------------- ExternalFile ---------------------------------------//
    public static final String RECORD_NAME_EXTERNAL_FILE = "ExternalFileRecordType";
    public static final int EXTERNAL_FILE_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int EXTERNAL_FILE_ARECORD_DATASET_NAME_FIELD_INDEX = 1;
    public static final int EXTERNAL_FILE_ARECORD_FILE_NUMBER_FIELD_INDEX = 2;
    public static final int EXTERNAL_FILE_ARECORD_FILE_NAME_FIELD_INDEX = 3;
    public static final int EXTERNAL_FILE_ARECORD_FILE_SIZE_FIELD_INDEX = 4;
    public static final int EXTERNAL_FILE_ARECORD_FILE_MOD_DATE_FIELD_INDEX = 5;
    public static final int EXTERNAL_FILE_ARECORD_FILE_PENDING_OP_FIELD_INDEX = 6;
    public static final ARecordType EXTERNAL_FILE_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_EXTERNAL_FILE,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_DATASET_NAME, FIELD_NAME_FILE_NUMBER,
                    FIELD_NAME_FILE_NAME, FIELD_NAME_FILE_SIZE, FIELD_NAME_FILE_MOD_TIME, FIELD_NAME_PENDING_OP },
            // FieldTypes
            new IAType[] { ASTRING, ASTRING, BuiltinType.AINT32, ASTRING, BuiltinType.AINT64, BuiltinType.ADATETIME,
                    BuiltinType.AINT32 },
            //IsOpen?
            true);

    //-------------------------------------- Synonym ---------------------------------------//
    public static final String RECORD_NAME_SYNONYM = "SynonymRecordType";
    public static final int SYNONYM_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int SYNONYM_ARECORD_SYNONYMNAME_FIELD_INDEX = 1;
    public static final int SYNONYM_ARECORD_OBJECTDATAVERSENAME_FIELD_INDEX = 2;
    public static final int SYNONYM_ARECORD_OBJECTNAME_FIELD_INDEX = 3;
    public static final ARecordType SYNONYM_RECORDTYPE = createRecordType(
            // RecordTypeName
            RECORD_NAME_SYNONYM,
            // FieldNames
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_SYNONYM_NAME, FIELD_NAME_OBJECT_DATAVERSE_NAME,
                    FIELD_NAME_OBJECT_NAME },
            // FieldTypes
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING },
            //IsOpen?
            true);

    //-------------------------------------- FullText Config ---------------------------------------//

    // TTTTTTTTTTTTTTTTTTT TO will polish later
    // Ordered List or Unordered List?
    // Unordered list is an MULTISET?
    public static final int FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX = 0;
    public static final int FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX = 1;

    // FullText Filter
    public static final int FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX = 2;
    public static final int FULLTEXT_ENTITY_ARECORD_USED_BY_FT_CONFIGS_FIELD_INDEX = 3;

    // Stopword Filter
    public static final int FULLTEXT_ENTITY_ARECORD_STOPWORD_LIST_FIELD_INDEX = 4;

    // FullText Config
    public static final int FULLTEXT_ENTITY_ARECORD_FULLTEXT_CONFIG_TOKENIZER_FIELD_INDEX = 2;
    public static final int FULLTEXT_ENTITY_ARECORD_FULLTEXT_CONFIG_FILTERS_LIST_FIELD_INDEX = 3;

    public static final String RECORD_NAME_FULLTEXT_ENTITY = "FulltextEntityRecordType";
    public static final ARecordType FULLTEXT_ENTITY_RECORDTYPE = createRecordType(RECORD_NAME_FULLTEXT_ENTITY,
            // Only two fields are in common in both FulltextFilter and FulltextConfig
            // Other specific fields in FulltextFilter and FulltextConfig are OPEN
            new String[] { FIELD_NAME_FULLTEXT_CATEGORY, FIELD_NAME_FULLTEXT_ENTITY_NAME },
            new IAType[] { ASTRING, ASTRING }, true);

    // private members
    private MetadataRecordTypes() {
    }

    public static ARecordType createRecordType(String recordTypeName, String[] fieldNames, IAType[] fieldTypes,
            boolean isOpen) {
        ARecordType recordType = new ARecordType(recordTypeName, fieldNames, fieldTypes, isOpen);
        if (recordTypeName != null) {
            recordType.generateNestedDerivedTypeNames();
        }
        return recordType;
    }

    public static final ARecordType createPropertiesRecordType() {
        return createRecordType(
                // RecordTypeName
                null,
                // FieldNames
                new String[] { FIELD_NAME_NAME, FIELD_NAME_VALUE },
                // FieldTypes
                new IAType[] { ASTRING, ASTRING },
                //IsOpen? Seriously?
                true);
    }
}
