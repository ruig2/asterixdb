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

package org.apache.asterix.metadata.entitytupletranslators;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_FILTER_CATEGORY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_FILTER_PIPELINE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_STOPWORD_LIST;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_TOKENIZER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_USED_BY_CONFIGS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_USED_BY_INDICES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IFullTextConfig;
import org.apache.asterix.metadata.api.IFullTextEntity;
import org.apache.asterix.metadata.api.IFullTextEntity.FullTextEntityCategory;
import org.apache.asterix.metadata.api.IFullTextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.fulltext.StopwordFullTextFilter;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

import com.google.common.collect.ImmutableList;

public class FulltextEntityTupleTranslator extends AbstractTupleTranslator<IFullTextEntity> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FulltextEntityTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULLTEXT_ENTITY_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            // in progress...
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFullTextEntity createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        AString categoryAString =
                (AString) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX);

        FullTextEntityCategory category = FullTextEntityCategory.fromValue(categoryAString.getStringValue());
        switch (category) {
            case FILTER:
                AString typeAString =
                        (AString) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX);
                IFullTextFilter.FullTextFilterType kind =
                        IFullTextFilter.FullTextFilterType.getEnumIgnoreCase(typeAString.getStringValue());
                switch (kind) {
                    case STOPWORD:
                        return StopwordFullTextFilter.createFilterFromARecord(aRecord);
                    case SYNONYM:
                }
            case CONFIG:
                break;
        }

        // debug
        return new StopwordFullTextFilter("decoded_my_stopword_filter", ImmutableList.of("aaa", "bbb", "ccc"));
    }

    private void writeKeyAndValue2FieldVariables(String key, String value) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(key);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
    }

    private void writeFilterType2RecordBuilder(IFullTextFilter.FullTextFilterType type) throws HyracksDataException {
        writeKeyAndValue2FieldVariables(FIELD_NAME_FULLTEXT_FILTER_CATEGORY, type.name());

        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeOrderedList2RecordBuilder(String strFieldName, List<String> list) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(strFieldName);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (String s : list) {
            itemValue.reset();
            aString.setValue(s);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }

        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeStopwordFilter(StopwordFullTextFilter stopwordFilter) throws HyracksDataException {
        writeFilterType2RecordBuilder(stopwordFilter.getFilterKind());
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_USED_BY_CONFIGS, stopwordFilter.getUsedByFTConfigs());
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_STOPWORD_LIST, stopwordFilter.getStopwordList());
    }

    private void writeFulltextFilter(IFullTextFilter filter) throws HyracksDataException {
        switch (filter.getFilterKind()) {
            case STOPWORD:
                writeStopwordFilter((StopwordFullTextFilter) filter);
                break;
            case SYNONYM:
            default:
                throw new NotImplementedException();
        }

        return;
    }

    private void writeFulltextConfig(IFullTextConfig config) throws HyracksDataException {
        writeKeyAndValue2FieldVariables(FIELD_NAME_FULLTEXT_TOKENIZER, config.getTokenizer().getCategory().name());
        recordBuilder.addField(fieldName, fieldValue);

        List<String> filterNames = new ArrayList<>();
        for (IFullTextFilter f : config.getFilters()) {
            filterNames.add(f.getName());
        }
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_FILTER_PIPELINE, filterNames);

        List<String> indexNames = new ArrayList<>();
        for (Index i : config.getUsedByIndices()) {
            // include the dataverse and dataset name into the index name?
            indexNames.add(i.getIndexName());
        }
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_USED_BY_INDICES, indexNames);
    }

    private void writeIndex(FullTextEntityCategory category, String entityName, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        // Write the 2 primary-index key fields
        aString.setValue(category.name());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(entityName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(IFullTextEntity fullTextEntity)
            throws AlgebricksException, HyracksDataException {
        tupleBuilder.reset();

        writeIndex(fullTextEntity.getCategory(), fullTextEntity.getName(), tupleBuilder);

        /////////////////////////////////////////////////////////
        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULLTEXT_ENTITY_RECORDTYPE);

        fieldValue.reset();
        aString.setValue(fullTextEntity.getCategory().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(fullTextEntity.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX, fieldValue);

        switch (fullTextEntity.getCategory()) {
            case FILTER:
                writeFulltextFilter((IFullTextFilter) fullTextEntity);
                break;
            case CONFIG:
                writeFulltextConfig((IFullTextConfig) fullTextEntity);
                break;
            default:
                break;
        }

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public ITupleReference createTupleAsIndex(FullTextEntityCategory category, String entityName)
            throws HyracksDataException {
        // -1 to get the number of fields in index only
        ArrayTupleBuilder tupleBuilder =
                new ArrayTupleBuilder(MetadataPrimaryIndexes.FULLTEXT_ENTITY_DATASET.getFieldCount() - 1);
        writeIndex(category, entityName, tupleBuilder);

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
