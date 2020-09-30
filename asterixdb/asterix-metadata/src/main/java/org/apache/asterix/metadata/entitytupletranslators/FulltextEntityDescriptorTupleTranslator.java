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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
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

import com.google.common.collect.ImmutableList;

public class FulltextEntityDescriptorTupleTranslator extends AbstractTupleTranslator<IFullTextEntityDescriptor> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FulltextEntityDescriptorTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULLTEXT_ENTITY_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
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
                (AString) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX);

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
                return createConfigDescriptorFromARecord(aRecord);
        }

        return null;
    }

    public StopwordsFullTextFilterDescriptor createStopwordsFilterDescriptorFromARecord(ARecord aRecord) {
        String name = ((AString) aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX))
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

    public FullTextConfigDescriptor createConfigDescriptorFromARecord(ARecord aRecord) {
        String name = ((AString) aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX))
                        .getStringValue();

        IFullTextConfig.TokenizerCategory tokenizerCategory =
                EnumUtils.getEnumIgnoreCase(IFullTextConfig.TokenizerCategory.class,
                        ((AString) aRecord.getValueByPos(
                                MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_CONFIG_TOKENIZER_FIELD_INDEX))
                                        .getStringValue());

        List<String> filterNames = new ArrayList<>();
        IACursor filterNamesCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_CONFIG_FILTERS_LIST_FIELD_INDEX)))
                        .getCursor();
        while (filterNamesCursor.next()) {
            filterNames.add(((AString) filterNamesCursor.get()).getStringValue());
        }

        MetadataTransactionContext mdTxnCtx = null;
        ImmutableList.Builder<IFullTextFilterDescriptor> filterDescriptorsBuilder =
                ImmutableList.<IFullTextFilterDescriptor> builder();
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            for (String filterName : filterNames) {
                IFullTextFilterDescriptor filterDescriptor =
                        MetadataManager.INSTANCE.getFullTextFilterDescriptor(mdTxnCtx, filterName);
                filterDescriptorsBuilder.add(filterDescriptor);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (RemoteException | AlgebricksException e) {
            e.printStackTrace();
        }

        List<String> usedByIndices = new ArrayList<>();
        IACursor indexNamesCursor = ((AOrderedList) (aRecord.getValueByPos(
                MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_CONFIG_USED_BY_INDICES_FIELD_INDEX))).getCursor();
        while (indexNamesCursor.next()) {
            usedByIndices.add(((AString) indexNamesCursor.get()).getStringValue());
        }

        FullTextConfigDescriptor configDescriptor =
                new FullTextConfigDescriptor(name, tokenizerCategory, filterDescriptorsBuilder.build(), usedByIndices);
        return configDescriptor;
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

    private void writeFulltextConfig(IFullTextConfigDescriptor configDescriptor) throws HyracksDataException {
        writeKeyAndValue2FieldVariables(FIELD_NAME_FULLTEXT_TOKENIZER, configDescriptor.getTokenizerCategory().name());
        recordBuilder.addField(fieldName, fieldValue);

        List<String> filterNames = new ArrayList<>();
        for (IFullTextFilterDescriptor f : configDescriptor.getFilterDescriptors()) {
            filterNames.add(f.getName());
        }
        writeOrderedList2RecordBuilder(FIELD_NAME_FULLTEXT_FILTER_PIPELINE, filterNames);

        List<String> indexNames = new ArrayList<>();
        for (String s : configDescriptor.getUsedByIndices()) {
            // include the dataverse and dataset name into the index name?
            indexNames.add(s);
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
    public ITupleReference getTupleFromMetadataEntity(IFullTextEntityDescriptor fullTextEntityDescriptor)
            throws HyracksDataException {
        tupleBuilder.reset();

        writeIndex(fullTextEntityDescriptor.getCategory(), fullTextEntityDescriptor.getName(), tupleBuilder);

        /////////////////////////////////////////////////////////
        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULLTEXT_ENTITY_RECORDTYPE);

        fieldValue.reset();
        aString.setValue(fullTextEntityDescriptor.getCategory().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(fullTextEntityDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX, fieldValue);

        switch (fullTextEntityDescriptor.getCategory()) {
            case FILTER:
                writeFulltextFilter((IFullTextFilterDescriptor) fullTextEntityDescriptor);
                break;
            case CONFIG:
                writeFulltextConfig((IFullTextConfigDescriptor) fullTextEntityDescriptor);
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
