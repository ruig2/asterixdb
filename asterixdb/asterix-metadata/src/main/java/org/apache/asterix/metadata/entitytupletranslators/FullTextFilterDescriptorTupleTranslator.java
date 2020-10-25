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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_FILTER_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_STOPWORD_LIST;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_TYPE_FIELD_INDEX;

import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
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
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilter;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.StopwordsFullTextFilterDescriptor;

import com.google.common.collect.ImmutableList;

public class FullTextFilterDescriptorTupleTranslator extends AbstractTupleTranslator<IFullTextFilterDescriptor> {

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
    protected IFullTextFilterDescriptor createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        AString dataverseName = (AString) aRecord.getValueByPos(FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX);
        AString filterName = (AString) aRecord.getValueByPos(FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX);
        AString filterTypeAString = (AString) aRecord.getValueByPos(FULL_TEXT_ARECORD_FILTER_TYPE_FIELD_INDEX);

        IFullTextFilter.FullTextFilterType filterType =
                IFullTextFilter.FullTextFilterType.getEnumIgnoreCase(filterTypeAString.getStringValue());
        switch (filterType) {
            case STOPWORDS:
                return createStopwordsFilterDescriptorFromARecord(dataverseName, filterName, aRecord);
            case STEMMER:
            case SYNONYM:
            default:
                throw new AlgebricksException("Not supported yet");
        }
    }

    public StopwordsFullTextFilterDescriptor createStopwordsFilterDescriptorFromARecord(AString dataverseName,
            AString name, ARecord aRecord) {
        ImmutableList.Builder<String> stopwordsBuilder = ImmutableList.<String> builder();
        IACursor stopwordsCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_STOPWORD_LIST_FIELD_INDEX))).getCursor();
        while (stopwordsCursor.next()) {
            stopwordsBuilder.add(((AString) stopwordsCursor.get()).getStringValue());
        }

        StopwordsFullTextFilterDescriptor descriptor = new StopwordsFullTextFilterDescriptor(
                dataverseName.getStringValue(), name.getStringValue(), stopwordsBuilder.build());
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

    private void writeStopwordFilterDescriptor(StopwordsFullTextFilterDescriptor stopwordsFullTextFilterDescriptor)
            throws HyracksDataException {
        writeOrderedList2RecordBuilder(FIELD_NAME_FULL_TEXT_STOPWORD_LIST,
                stopwordsFullTextFilterDescriptor.getStopwordList());
    }

    private void writeFulltextFilter(IFullTextFilterDescriptor filterDescriptor) throws HyracksDataException {
        fieldValue.reset();
        aString.setValue(filterDescriptor.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(filterDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_FILTER_NAME_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(filterDescriptor.getFilterType().getValue());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_FILTER_TYPE_FIELD_INDEX, fieldValue);

        switch (filterDescriptor.getFilterType()) {
            case STOPWORDS:
                writeStopwordFilterDescriptor((StopwordsFullTextFilterDescriptor) filterDescriptor);
                break;
            case STEMMER:
            case SYNONYM:
            default:
                throw new NotImplementedException();
        }
    }

    private void writeIndex(String dataverseName, String filterName, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(filterName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(IFullTextFilterDescriptor filterDescriptor)
            throws HyracksDataException {
        tupleBuilder.reset();

        writeIndex(filterDescriptor.getDataverseName(), filterDescriptor.getName(), tupleBuilder);

        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULL_TEXT_FILTER_RECORDTYPE);

        writeFulltextFilter(filterDescriptor);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public ITupleReference createTupleAsIndex(String dataverseName, String filterName) throws HyracksDataException {
        // -1 to get the number of fields in index only
        ArrayTupleBuilder tupleBuilder =
                new ArrayTupleBuilder(MetadataPrimaryIndexes.FULL_TEXT_FILTER_DATASET.getFieldCount() - 1);
        writeIndex(dataverseName, filterName, tupleBuilder);

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
