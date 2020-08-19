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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListFrameTupleAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.InvertedListFrameTupleAppender;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * This is a fixed-size tuple accessor class.
 * The frame structure: [4 bytes for minimum Hyracks frame count] [fixed-size tuple 1] ... [fixed-size tuple n] ...
 * [4 bytes for the tuple count in a frame]
 */
public class VariableSizeInvertedListFrameTupleAccessor extends AbstractInvertedListFrameTupleAccessor {

    private int[] tupleStartOffsets;
    private int tupleCount;
    private int lastTupleLen;

    public VariableSizeInvertedListFrameTupleAccessor(int frameSize, ITypeTraits[] fields) {
        super(frameSize, fields);
    }

    @Override
    protected void verifyTypeTraits() {
        InvertedIndexUtils.verifyHasVarSizeTypeTrait(fields);
    }

    @Override
    public void reset(ByteBuffer buffer) {
        super.reset(buffer);

        tupleCount = getTupleCount();
        tupleStartOffsets = new int[tupleCount];

        if (tupleCount > 0) {
            int startOff = InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE;
            int pos = startOff;
            tupleStartOffsets[0] = 0;
            // If there is only one tuple, store the tuple length in lastTupleLen
            lastTupleLen = UTF8StringUtil.getUTFStringFieldLength(buffer.array(), pos);

            for (int i = 1; i < tupleCount; i++) {
                int len = UTF8StringUtil.getUTFStringFieldLength(buffer.array(), pos);
                tupleStartOffsets[i] = tupleStartOffsets[i - 1] + len;
                if (i == tupleCount - 1) {
                    lastTupleLen = len;
                }

                pos += len;
            }
        }
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE + tupleStartOffsets[tupleIndex];
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        if (tupleIndex == fields.length - 1) {
            return InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE + tupleStartOffsets[tupleIndex] + lastTupleLen;
        } else if (tupleIndex < 0) {
            return InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE;
        }
        return InvertedListFrameTupleAppender.MINFRAME_COUNT_SIZE + tupleStartOffsets[tupleIndex + 1];
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
        int pos = getTupleStartOffset(tupleIndex);
        for (int i = 0; i < fIdx; i++) {
            pos += UTF8StringUtil.getUTFStringFieldLength(buffer.array(), pos);
        }
        return pos;
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        int pos = getTupleStartOffset(tupleIndex);
        for (int i = 0; i <= fIdx; i++) {
            pos += UTF8StringUtil.getUTFStringFieldLength(buffer.array(), pos);
        }
        return pos;
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }
}
