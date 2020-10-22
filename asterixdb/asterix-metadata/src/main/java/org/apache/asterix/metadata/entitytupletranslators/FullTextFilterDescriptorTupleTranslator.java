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

import com.google.common.collect.ImmutableList;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_FILTER_CATEGORY;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_STOPWORD_LIST;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_TOKENIZER;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_USED_BY_CONFIGS;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULLTEXT_USED_BY_INDICES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_FILTER_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextConfigDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextEntity.FullTextEntityCategory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextEntityDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilter;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.StopwordsFullTextFilterDescriptor;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

public class FullTextFilterDescriptorTupleTranslator extends AbstractTupleTranslator<IFullTextEntityDescriptor> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FullTextFilterDescriptorTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULL_TEXT_FILTER_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFullTextEntityDescriptor createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        AString categoryAString =
                (AString) aRecord.getValueByPos(FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX);

        FullTextEntityCategory category = FullTextEntityCategory.getEnumIgnoreCase(categoryAString.getStringValue());
        switch (category) {
            case FILTER:
                AString typeAString =
                        (AString) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX);
                IFullTextFilter.FullTextFilterType kind =
                        IFullTextFilter.FullTextFilterType.getEnumIgnoreCase(typeAString.getStringValue());
                switch (kind) {
                    case STOPWORDS:
                        return createStopwordsFilterDescriptorFromARecord(aRecord);
                    case STEMMER:
                    case SYNONYM:
                    default:
                        throw new AlgebricksException("Not supported yet");
                }
            case CONFIG:
                // return createConfigDescriptorFromARecord(aRecord);
        }

        return null;
    }

    public StopwordsFullTextFilterDescriptor createStopwordsFilterDescriptorFromARecord(ARecord aRecord) {
        String name = ((AString) aRecord
                .getValueByPos(FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX))
                        .getStringValue();
        ImmutableList.Builder<String> stopwordsBuilder = ImmutableList.<String> builder();
        IACursor stopwordsCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_STOPWORD_LIST_FIELD_INDEX))).getCursor();
        while (stopwordsCursor.next()) {
            stopwordsBuilder.add(((AString) stopwordsCursor.get()).getStringValue());
        }

        IACursor usedByConfigsCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_USED_BY_FT_CONFIGS_FIELD_INDEX)))
                        .getCursor();
        List<String> usedByConfigs = new ArrayList<>();
        while (usedByConfigsCursor.next()) {
            usedByConfigs.add(((AString) usedByConfigsCursor.get()).getStringValue());
        }

        StopwordsFullTextFilterDescriptor descriptor =
                new StopwordsFullTextFilterDescriptor(name, stopwordsBuilder.build(), usedByConfigs);
        return descriptor;
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

    private void writeFilterDescriptorBasic(IFullTextFilterDescriptor filterDescriptor) throws HyracksDataException {
        writeFilterType2RecordBuilder(filterDescriptor.getFilterType());
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_USED_BY_CONFIGS, filterDescriptor.getUsedByConfigs());
    }

    private void writeStopwordFilterDescriptor(StopwordsFullTextFilterDescriptor stopwordsFullTextFilterDescriptor)
            throws HyracksDataException {
        writeFilterDescriptorBasic(stopwordsFullTextFilterDescriptor);
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_STOPWORD_LIST,
                stopwordsFullTextFilterDescriptor.getStopwordList());
    }

    private void writeFulltextFilter(IFullTextFilterDescriptor filterDescriptor) throws HyracksDataException {
        switch (filterDescriptor.getFilterType()) {
            case STOPWORDS:
                writeStopwordFilterDescriptor((StopwordsFullTextFilterDescriptor) filterDescriptor);
                break;
            case STEMMER:
            case SYNONYM:
            default:
                throw new NotImplementedException();
        }

        return;
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
    public ITupleReference getTupleFromMetadataEntity(IFullTextEntityDescriptor fullTextEntityDescriptor)
            throws HyracksDataException {
        tupleBuilder.reset();

        writeIndex(fullTextEntityDescriptor.getCategory(), fullTextEntityDescriptor.getName(), tupleBuilder);

        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULL_TEXT_FILTER_RECORDTYPE);

        fieldValue.reset();
        aString.setValue(fullTextEntityDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX, fieldValue);

        switch (fullTextEntityDescriptor.getCategory()) {
            case FILTER:
                writeFulltextFilter((IFullTextFilterDescriptor) fullTextEntityDescriptor);
                break;
            case CONFIG:
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
                new ArrayTupleBuilder(MetadataPrimaryIndexes.FULL_TEXT_FILTER_DATASET.getFieldCount() - 1);
        writeIndex(category, entityName, tupleBuilder);

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
