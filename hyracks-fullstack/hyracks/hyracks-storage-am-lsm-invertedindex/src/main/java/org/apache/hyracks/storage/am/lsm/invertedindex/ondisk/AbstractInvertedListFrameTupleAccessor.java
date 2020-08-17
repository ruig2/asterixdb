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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;

/**
 * This is a frame tuple accessor class for inverted list.
 * The frame structure: [4 bytes for minimum Hyracks frame count] [fixed-size tuple 1] ... [fixed-size tuple n] ...
 * [4 bytes for the tuple count in a frame]
 */
public abstract class AbstractInvertedListFrameTupleAccessor implements IFrameTupleAccessor {

    protected final int frameSize;
    protected ByteBuffer buffer;

    protected final ITypeTraits[] fields;
    protected final int[] fieldStartOffsets;

    protected abstract void verifyTypeTraits();

    public AbstractInvertedListFrameTupleAccessor(int frameSize, ITypeTraits[] fields) {
        this.frameSize = frameSize;
        this.fields = fields;
        this.fieldStartOffsets = new int[fields.length];
        this.fieldStartOffsets[0] = 0;

        verifyTypeTraits();
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return fields[fIdx].getFixedLength();
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getFieldSlotsLength() {
        return 0;
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + fieldStartOffsets[fIdx];
    }

    @Override
    public int getTupleCount() {
        return buffer != null ? buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)) : 0;
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return getFieldEndOffset(tupleIndex, fields.length - 1);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        // return InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE + tupleIndex * tupleSize;
        throw new NotImplementedException();
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }
}
