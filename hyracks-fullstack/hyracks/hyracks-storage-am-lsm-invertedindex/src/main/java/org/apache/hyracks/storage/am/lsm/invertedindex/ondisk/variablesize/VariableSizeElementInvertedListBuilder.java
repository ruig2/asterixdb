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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

public class VariableSizeElementInvertedListBuilder extends AInvertedListBuilder {

    public VariableSizeElementInvertedListBuilder(ITypeTraits[] invListFields) {
        super(invListFields);
        InvertedIndexUtils.verifyHasVarSizeTypeTrait(invListFields);
    }

    @Override
    public boolean startNewList(ITupleReference tuple, int tokenField) {
        listSize = 0;
        return true;
    }

    private boolean checkEnoughSpace(ITupleReference tuple, int numTokenFields, int numElementFields) {
        int lenFields = 0;
        for (int i = 0; i < numElementFields; i++) {
            int field = numTokenFields + i;
            lenFields += tuple.getFieldLength(field);
        }
        if (pos + lenFields > targetBuf.length) {
            return false;
        }

        return true;
    }

    @Override
    public boolean appendElement(ITupleReference tuple, int numTokenFields, int numElementFields) {

        if (checkEnoughSpace(tuple, numTokenFields, numElementFields) == false) {
            return false;
        }

        for (int i = 0; i < numElementFields; i++) {
            int field = numTokenFields + i;
            int lenField = tuple.getFieldLength(field);
            System.arraycopy(tuple.getFieldData(field), tuple.getFieldStart(field), targetBuf, pos, lenField);
            pos += lenField;
        }
        listSize++;

        return true;
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}