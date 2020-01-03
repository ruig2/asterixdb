package org.apache.asterix.metadata.entitytupletranslators;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IFulltextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.entities.fulltext.StopwordFulltextFilter;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FulltextFilterTupleTranslator extends AbstractTupleTranslator<IFulltextFilter> {

    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);


    protected FulltextFilterTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULLTEXT_CONFIG_DATASET, FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            // in progress...
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected IFulltextFilter createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        // in progress...
        return new StopwordFulltextFilter(
                "my_stopword_filter",
                ImmutableList.of("a", "an", "the")
        );
    }

    @Override public ITupleReference getTupleFromMetadataEntity(IFulltextFilter filter)
            throws AlgebricksException, HyracksDataException {
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

        switch (filter.getType()) {
            case STOPWORD:
                break;
            case SYNONYM:
                break;
            default:
                break;
        }

        // place-holder
        int8Serde.serialize(new AInt8((byte)8), tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // in progress...
        /*
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
         */

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
