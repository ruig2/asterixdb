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

import org.apache.asterix.metadata.api.IFulltextEntity.FulltextEntityCategory;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX;

import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IFulltextConfig;
import org.apache.asterix.metadata.api.IFulltextEntity;
import org.apache.asterix.metadata.api.IFulltextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX;
import org.apache.asterix.metadata.entities.fulltext.StopwordFulltextFilter;
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

public class FulltextEntityTupleTranslator extends AbstractTupleTranslator<IFulltextEntity> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
    protected AInt8 categoryIdAInt8 = null;

    protected FulltextEntityTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULLTEXT_CONFIG_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            // in progress...
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFulltextEntity createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        AInt8 categoryIdAInt8 =
                (AInt8) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX);

        FulltextEntityCategory category = FulltextEntityCategory.fromId(categoryIdAInt8.getByteValue());
        switch (category) {
            case FULLTEXT_FILTER:
                AInt8 kindAInt8 = (AInt8) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_FILTER_KIND_FIELD_INDEX);
                IFulltextFilter.FulltextFilterKind kind = IFulltextFilter.FulltextFilterKind.fromId(kindAInt8.getByteValue());
                switch (kind) {
                    case STOPWORD:
                        return StopwordFulltextFilter.createFilterFromARecord(aRecord);
                    case SYNONYM:
                }
            case FULLTEXT_CONFIG:
                break;
        }

        return new StopwordFulltextFilter("decoded_my_stopword_filter", ImmutableList.of("aaa", "bbb", "ccc"));
    }

    private void writeFilterCategory2RecordBuilder(IFulltextFilter.FulltextFilterKind category)
            throws HyracksDataException {
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_FULLTEXT_FILTER_CATEGORY);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        fieldValue.reset();
        AInt8 idAInt8 = new AInt8(category.getId());
        int8Serde.serialize(idAInt8, fieldValue.getDataOutput());

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

    private void writeStopwordFilter(StopwordFulltextFilter stopwordFilter) throws HyracksDataException {
        writeFilterCategory2RecordBuilder(stopwordFilter.getFilterKind());
        writeOrderedList2RecordBuilder("UsedByFTConfigs", stopwordFilter.getUsedByFTConfigs());
        writeOrderedList2RecordBuilder("StopwordList", stopwordFilter.getStopwordList());
    }

    private void writeFulltextFilter(IFulltextFilter filter) throws HyracksDataException {
        switch (filter.getFilterKind()) {
            case STOPWORD:
                writeStopwordFilter((StopwordFulltextFilter) filter);
                break;
            case SYNONYM:
            default:
                throw new NotImplementedException();
        }

        return;
    }

    private void getTupleForFulltextConfig(IFulltextConfig config) {
    }

    private void writeIndex(IFulltextEntity.FulltextEntityCategory category, String entityName, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        // Write the 2 primary-index key fields
        byte categoryId = category.getId();
        categoryIdAInt8 = new AInt8(categoryId);
        int8Serde.serialize(categoryIdAInt8, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(entityName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(IFulltextEntity fulltextEntity)
            throws AlgebricksException, HyracksDataException {
        tupleBuilder.reset();

        writeIndex(fulltextEntity.getCategory(), fulltextEntity.getName(), tupleBuilder);

        /////////////////////////////////////////////////////////
        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULLTEXT_CONFIG_RECORDTYPE);

        fieldValue.reset();
        int8Serde.serialize(categoryIdAInt8, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_CATEGORY_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(fulltextEntity.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX, fieldValue);

        switch (fulltextEntity.getCategory()) {
            case FULLTEXT_FILTER:
                writeFulltextFilter((IFulltextFilter) fulltextEntity);
                break;
            case FULLTEXT_CONFIG:
                getTupleForFulltextConfig((IFulltextConfig) fulltextEntity);
                break;
            default:
                break;
        }

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public ITupleReference createTupleAsIndex(IFulltextEntity.FulltextEntityCategory category, String entityName)
            throws HyracksDataException {
        // -1 to get the number of fields in index only
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(MetadataPrimaryIndexes.FULLTEXT_CONFIG_DATASET.getFieldCount() - 1);
        writeIndex(category, entityName, tupleBuilder);

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
