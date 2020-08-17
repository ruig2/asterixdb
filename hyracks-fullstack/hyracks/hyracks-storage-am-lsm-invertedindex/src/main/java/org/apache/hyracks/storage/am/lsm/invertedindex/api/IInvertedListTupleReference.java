package org.apache.hyracks.storage.am.lsm.invertedindex.api;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IInvertedListTupleReference extends ITupleReference {
    void reset(byte[] data, int startOff);
}
