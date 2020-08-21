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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;

import java.nio.ByteBuffer;

public class VariableSizeInvertedListTupleReference extends AbstractInvertedListTupleReference {

    private int lenLastField;

    @Override
    protected void verifyTypeTrait() {
        InvertedIndexUtils.verifyHasVarSizeTypeTrait(typeTraits);
    }

    private void verifyFieldTypeTag(ITypeTraits typeTrait, int tag) {
        if (!typeTrait.isFixedLength() && tag != 13) {
            throw new UnsupportedOperationException("For variable-size type trait, only string is supported");
        }
    }

    public VariableSizeInvertedListTupleReference(ITypeTraits[] typeTraits) {
        super(typeTraits);
    }

    @Override
    protected void calculateFieldStartOffsets() {
        int tmpPos = startOff;
        this.fieldStartOffsets[0] = 0;

        verifyFieldTypeTag(typeTraits[0], data[tmpPos]);
        int lenField = UTF8StringUtil.getUTFStringFieldLength(data, tmpPos);
        lenLastField = lenField;

        for (int i = 1; i < typeTraits.length; i++) {
            if (typeTraits[i - 1].isFixedLength()) {
                fieldStartOffsets[i] = fieldStartOffsets[i - 1] + typeTraits[i - 1].getFixedLength();
            } else {
                // 13 is the type tag of ATypeTag.String which is defined in the upper AsterixDB layer
                // ToDo: find a better way to handle ATypeTag.String

                verifyFieldTypeTag(typeTraits[i], data[tmpPos]);
                lenField = UTF8StringUtil.getUTFStringFieldLength(data, tmpPos);
                fieldStartOffsets[i] = fieldStartOffsets[i - 1] + lenField;

                if (i == typeTraits.length - 1) {
                    lenLastField = lenField;
                }
                tmpPos += lenField;
            }
        }
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
    public int getFieldLength(int fIdx) {
        if (fIdx == typeTraits.length - 1) {
            return lenLastField;
        } else {
            return fieldStartOffsets[fIdx + 1] - fieldStartOffsets[fIdx];
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        return startOff + fieldStartOffsets[fIdx];
    }

    @Override public String toString() {
        String result = "";

        for (int i = 0; i < typeTraits.length; i++) {
            int pos = getFieldStart(i);
            if (typeTraits[i].isFixedLength()) {
                int len = typeTraits[i].getFixedLength();
                result += ByteBuffer.wrap(data, pos, len).getInt() + ", ";
            } else {
                StringBuilder builder = new StringBuilder();
                // pos + 1 to skip the type tag
                result += UTF8StringUtil.toString(builder, data, pos+1).toString() + ", ";
            }
        }
        return result;
    }
}
