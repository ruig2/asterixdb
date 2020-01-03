package org.apache.asterix.metadata.entitytupletranslators;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IFulltextBasic;
import org.apache.asterix.metadata.api.IFulltextConfig;
import org.apache.asterix.metadata.api.IFulltextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_CATEGORY_FIELD_INDEX;
import org.apache.asterix.metadata.entities.fulltext.StopwordFulltextFilter;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

import java.util.List;
import java.util.Map;

public class FulltextBasicTupleTranslator extends AbstractTupleTranslator<IFulltextBasic> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FulltextBasicTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULLTEXT_CONFIG_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            // in progress...
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFulltextBasic createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        // in progress...
        return new StopwordFulltextFilter(
                "my_stopword_filter",
                ImmutableList.of("a", "an", "the")
        );
    }

    private void writeFilterCategory2RecordBuilder(IFulltextFilter.FulltextFilterCategory category)
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

        OrderedListBuilder listBuilder  = new OrderedListBuilder();
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
        writeFilterCategory2RecordBuilder(stopwordFilter.getFilterCategory());
        writeOrderedList2RecordBuilder("UsedByFTConfigs", stopwordFilter.getUsedByFTConfigs());
        writeOrderedList2RecordBuilder("StopwordList", stopwordFilter.getStopwordList());
    }

    private void writeFulltextFilter(IFulltextFilter filter) throws HyracksDataException {
        switch (filter.getFilterCategory()) {
            case STOPWORD:
                writeStopwordFilter( (StopwordFulltextFilter) filter );
                break;
            case SYNONYM:
            default:
                throw new NotImplementedException();
        }

        return;
    }

    private void getTupleForFulltextConfig(IFulltextConfig config) {
    }


    @Override
    public ITupleReference getTupleFromMetadataEntity(IFulltextBasic fulltextBasic)
            throws AlgebricksException, HyracksDataException {
        tupleBuilder.reset();

        // Write the 2 primary-index key fields
        byte categoryId = fulltextBasic.getCategory().getId();
        AInt8 categoryIdAInt8 = new AInt8(categoryId);
        int8Serde.serialize(categoryIdAInt8, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(fulltextBasic.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        /////////////////////////////////////////////////////////
        // Write the record
        recordBuilder.reset(MetadataRecordTypes.FULLTEXT_CONFIG_RECORDTYPE);

        fieldValue.reset();
        int8Serde.serialize(categoryIdAInt8, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_CATEGORY_FIELD_INDEX, fieldValue);

        fieldValue.reset();
        aString.setValue(fulltextBasic.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULLTEXT_ENTITY_ARECORD_FULLTEXT_CATEGORY_FIELD_INDEX, fieldValue);

        switch (fulltextBasic.getCategory()) {
            case FULLTEXT_FILTER:
                writeFulltextFilter((IFulltextFilter) fulltextBasic);
                break;
            case FULLTEXT_CONFIG:
                getTupleForFulltextConfig((IFulltextConfig) fulltextBasic);
                break;
            default:
                break;
        }

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
