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

public class VariableSizeInvertedListTupleReference extends AbstractInvertedListTupleReference {

    private int lenLastField;

    @Override
    protected void verifyTypeTrait() {
        InvertedIndexUtils.verifyHasVarSizeTypeTrait(typeTraits);
    }

    public VariableSizeInvertedListTupleReference(ITypeTraits[] typeTraits) {
        super(typeTraits);
    }

    @Override
    protected void calculateFieldStartOffsets() {
        this.fieldStartOffsets[0] = 0;
        if (data[0] == 13) {
            int lenField = UTF8StringUtil.getUTFStringFieldLength(data, 0);
            lenLastField = lenField;
        }

        for (int i = 1; i < typeTraits.length; i++) {
            if (typeTraits[i - 1].isFixedLength()) {
                fieldStartOffsets[i] = fieldStartOffsets[i - 1] + typeTraits[i - 1].getFixedLength();
            } else {
                // 13 is the type tag of ATypeTag.String which is defined in the upper AsterixDB layer
                // ToDo: find a better way to handle ATypeTag.String
                int tmpPos = startOff + fieldStartOffsets[i - 1];
                if (data[tmpPos] == 13) {
                    int lenField = UTF8StringUtil.getUTFStringFieldLength(data, tmpPos);
                    fieldStartOffsets[i] = fieldStartOffsets[i - 1] + lenField;

                    if (i == typeTraits.length - 1) {
                        lenLastField = lenField;
                    }
                } else {
                    // ToDo: support other types (non-string) later
                    throw new UnsupportedOperationException();
                }
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
            System.out.println("zzzzzzzzzzzz " + (fieldStartOffsets[fIdx + 1] - fieldStartOffsets[fIdx]));
            return fieldStartOffsets[fIdx + 1] - fieldStartOffsets[fIdx];
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        return startOff + fieldStartOffsets[fIdx];
    }
}
