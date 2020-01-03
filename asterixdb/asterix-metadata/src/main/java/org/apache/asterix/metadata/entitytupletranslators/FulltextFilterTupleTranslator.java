package org.apache.asterix.metadata.entitytupletranslators;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.metadata.api.IFulltextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.entities.fulltext.StopwordFulltextFilter;
import org.apache.asterix.om.base.ARecord;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FulltextFilterTupleTranslator extends AbstractTupleTranslator<IFulltextFilter> {

    // ????????? Should this be 2 or 3?
    private static final int FULLTEXT_FILTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;

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
        aString.setValue("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
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
