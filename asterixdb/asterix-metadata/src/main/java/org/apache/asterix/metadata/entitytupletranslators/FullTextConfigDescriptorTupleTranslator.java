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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.DataverseName;
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
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextConfigDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterDescriptor;

import com.google.common.collect.ImmutableList;

public class FullTextConfigDescriptorTupleTranslator extends AbstractTupleTranslator<IFullTextConfigDescriptor> {

    private static final int FULL_TEXT_CONFIG_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FullTextConfigDescriptorTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULL_TEXT_CONFIG_DATASET, FULL_TEXT_CONFIG_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFullTextConfigDescriptor createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        String dataverseName =
                ((AString) aRecord.getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();

        String name = ((AString) aRecord.getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX))
                .getStringValue();

        IFullTextConfig.TokenizerCategory tokenizerCategory =
                EnumUtils.getEnumIgnoreCase(IFullTextConfig.TokenizerCategory.class,
                        ((AString) aRecord
                                .getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX))
                                        .getStringValue());

        List<String> filterNames = new ArrayList<>();
        IACursor filterNamesCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX))).getCursor();
        while (filterNamesCursor.next()) {
            filterNames.add(((AString) filterNamesCursor.get()).getStringValue());
        }

        // ToDo: where to start metadata txn?
        MetadataTransactionContext mdTxnCtx = null;
        ImmutableList.Builder<IFullTextFilterDescriptor> filterDescriptorsBuilder =
                ImmutableList.<IFullTextFilterDescriptor> builder();
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            for (String filterName : filterNames) {
                IFullTextFilterDescriptor filterDescriptor = MetadataManager.INSTANCE.getFullTextFilterDescriptor(
                        mdTxnCtx, DataverseName.createFromCanonicalForm(dataverseName), filterName);
                filterDescriptorsBuilder.add(filterDescriptor);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (RemoteException | AlgebricksException e) {
            try {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (RemoteException remoteException) {
                throw new MetadataException(ErrorCode.FULL_TEXT_FAIL_TO_GET_FILTER_FROM_METADATA, remoteException);
            }
        }

        FullTextConfigDescriptor configDescriptor =
                new FullTextConfigDescriptor(dataverseName, name, tokenizerCategory, filterDescriptorsBuilder.build());
        return configDescriptor;
    }

    private void writeIndex(String dataverseName, String configName, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(configName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(IFullTextConfigDescriptor fullTextConfigDescriptor)
            throws HyracksDataException {
        tupleBuilder.reset();

        writeIndex(fullTextConfigDescriptor.getDataverseName(), fullTextConfigDescriptor.getName(), tupleBuilder);

        recordBuilder.reset(MetadataRecordTypes.FULL_TEXT_CONFIG_RECORDTYPE);

        // write dataverse name
        fieldValue.reset();
        aString.setValue(fullTextConfigDescriptor.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write name
        fieldValue.reset();
        aString.setValue(fullTextConfigDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX, fieldValue);

        // write tokenizer category
        fieldValue.reset();
        aString.setValue(fullTextConfigDescriptor.getTokenizerCategory().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX, fieldValue);

        // set filter pipeline
        List<String> filterNames = new ArrayList<>();
        for (IFullTextFilterDescriptor f : fullTextConfigDescriptor.getFilterDescriptors()) {
            filterNames.add(f.getName());
        }

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (String s : filterNames) {
            itemValue.reset();
            aString.setValue(s);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }

        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public ITupleReference createTupleAsIndex(String dataverseName, String configName) throws HyracksDataException {
        // -1 to get the number of fields in index only
        ArrayTupleBuilder tupleBuilder =
                new ArrayTupleBuilder(MetadataPrimaryIndexes.FULL_TEXT_CONFIG_DATASET.getFieldCount() - 1);
        writeIndex(dataverseName, configName, tupleBuilder);

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
