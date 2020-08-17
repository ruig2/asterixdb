package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListTupleReference;

public abstract class AbstractInvertedListTupleReference implements IInvertedListTupleReference {

    protected final ITypeTraits[] typeTraits;
    protected final int[] fieldStartOffsets;
    protected byte[] data;
    protected int startOff;

    // check if the type trait is fixed-size or variable-size
    // throws an IllegalArgument exception if get unexpected traits
    protected abstract void checkTypeTrait();

    public AbstractInvertedListTupleReference(ITypeTraits[] typeTraits) {
        this.typeTraits = typeTraits;
        this.fieldStartOffsets = new int[typeTraits.length];
        this.fieldStartOffsets[0] = 0;

        checkTypeTrait();
    }

    protected abstract void calculateFieldStartOffsets();

    @Override
    public void reset(byte[] data, int startOff) {
        this.data = data;
        this.startOff = startOff;
        calculateFieldStartOffsets();
    }

    @Override
    public int getFieldCount() {
        return typeTraits.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return startOff + fieldStartOffsets[fIdx];
    }
}
