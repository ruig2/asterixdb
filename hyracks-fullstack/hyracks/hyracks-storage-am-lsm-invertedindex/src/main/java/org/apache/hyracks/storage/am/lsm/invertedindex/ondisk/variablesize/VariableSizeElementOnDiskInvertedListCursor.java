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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.variablesize;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.AbstractOnDiskInvertedListCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.string.UTF8StringUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

/**
 * A cursor class that traverse an inverted list that consists of fixed-size elements on disk
 *
 */

public class VariableSizeElementOnDiskInvertedListCursor extends AbstractOnDiskInvertedListCursor {

    private boolean isInit;

    public VariableSizeElementOnDiskInvertedListCursor(IBufferCache bufferCache, int fileId,
            ITypeTraits[] invListFields, IHyracksTaskContext ctx, IIndexCursorStats stats) throws HyracksDataException {
        super(bufferCache, fileId, invListFields, ctx, stats);
        isInit = true;
    }

    @Override protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred)
            throws HyracksDataException {
        super.doOpen(initialState, searchPred);
        currentElementIxForScan = 0;
    }

    /**
     * Returns the next element.
     */
    @Override
    public void doNext() throws HyracksDataException {
        // init state
        if (isInit) {
            isInit = false;
        } else {
            int lenCurrentTuple = -1;
            int bufferLen = buffers.get(currentPageIxForScan).array().length;
            if (currentOffsetForScan < bufferLen) {
                lenCurrentTuple = UTF8StringUtil.getUTFStringFieldLength(buffers.get(currentPageIxForScan).array(),
                        currentOffsetForScan);
            }
            // !!! assume the empty tailing space in a frame is filled with 0
            if (lenCurrentTuple > 0) {
                currentOffsetForScan += lenCurrentTuple;
            } else {
                currentPageIxForScan++;
                currentOffsetForScan = 0;
            }
        }

        // Needs to read the next block?
        if (currentOffsetForScan >= buffers.size() && endPageId > bufferEndPageId) {
            loadPages();
            currentOffsetForScan = 0;
        }

        currentElementIxForScan++;
        tuple.reset(buffers.get(currentPageIxForScan).array(), currentOffsetForScan);
    }

    /**
     * Updates the information about this block.
     */
    @Override
    protected void setBlockInfo() {
        super.setBlockInfo();
        currentOffsetForScan = bufferStartElementIx == 0 ? startOff : 0;
        isInit = true;
    }

    /**
     * Checks whether the given tuple exists on this inverted list. This method is used when doing a random traversal.
     */
    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        if (!isInit) {
            next();
        }
        while (hasNext()) {
            int cmp = invListCmp.compare(searchTuple, tuple);
            if (cmp < 0) {
                return false;
            } else if (cmp == 0) {
                return true;
            }
            // ToDo: here we get the tuple first and then call next() later because the upper-layer caller in InvertedListMerger does so
            // However, this is not consistent with other use cases of next() in AsterixDB
            // Maybe we need to fix the InvertedListMerger part to call next() first then getTuple() to follow the convention to use cursor
            next();
        }

        return false;
    }

    /**
     * Opens the cursor for the given inverted list. After this open() call, prepreLoadPages() should be called
     * before loadPages() are called. For more details, check prepapreLoadPages().
     */
    @Override
    protected void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException {
        super.setInvListInfo(startPageId, endPageId, startOff, numElements);

        this.currentOffsetForScan = startOff;
    }

    // Debugging purpose
    @SuppressWarnings("rawtypes")
    @Override public String toString() {

        int oldCurrentOff = currentOffsetForScan;
        int oldCurrentPageId = currentPageIxForScan;
        int oldCurrentElementIx = currentElementIxForScan;
        boolean oldIsInit = isInit;

        currentOffsetForScan = startOff;
        currentPageIxForScan = 0;
        currentElementIxForScan = 0;
        isInit = false;

        String result = "";
        try {
            while (hasNext()) {
                next();

                if (currentElementIxForScan - 1 == oldCurrentElementIx) {
                    result += "->";
                }

                ITupleReference tuple = getTuple();
                for (int i = 0; i < tuple.getFieldCount(); i++) {
                    int pos = tuple.getFieldStart(i);
                    if (invListFields[i].isFixedLength()) {
                        int len = invListFields[i].getFixedLength();
                        result += ByteBuffer.wrap(tuple.getFieldData(i), pos, len).getInt() + ", ";
                    } else {
                        StringBuilder builder = new StringBuilder();
                        // pos + 1 to skip the type tag
                        result += UTF8StringUtil.toString(builder, tuple.getFieldData(i), pos+1).toString() + ", ";
                    }
                }
                result += " ";
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

        // reset previous state
        currentOffsetForScan = oldCurrentOff;
        currentPageIxForScan = oldCurrentPageId;
        currentElementIxForScan = oldCurrentElementIx;
        isInit = oldIsInit;

        return result;
    }

    /**
     * Prints the contents of the current inverted list (a debugging method).
     */
    @SuppressWarnings("rawtypes")
    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        /*
        int oldCurrentOff = currentOffsetForScan;
        int oldCurrentPageId = currentPageIxForScan;
        int oldCurrentElementIx = currentElementIxForScan;
        
        currentOffsetForScan = startOff - elementSize;
        currentPageIxForScan = 0;
        currentElementIxForScan = 0;
        
        StringBuilder strBuilder = new StringBuilder();
        
        while (hasNext()) {
            next();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i));
                DataInput dataIn = new DataInputStream(inStream);
                Object o = serdes[i].deserialize(dataIn);
                strBuilder.append(o.toString());
                if (i + 1 < tuple.getFieldCount()) {
                    strBuilder.append(",");
                }
            }
            strBuilder.append(" ");
        }
        
        // reset previous state
        currentOffsetForScan = oldCurrentOff;
        currentPageIxForScan = oldCurrentPageId;
        currentElementIxForScan = oldCurrentElementIx;
        
        return strBuilder.toString();
         */
        return "";
    }
}
