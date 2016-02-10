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

package org.apache.hyracks.storage.am.common.datagen;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

@SuppressWarnings("rawtypes")
public class TupleBatch {
    private final int size;
    private final TupleGenerator[] tupleGens;
    public final AtomicBoolean inUse = new AtomicBoolean(false);

    public TupleBatch(int size, IFieldValueGenerator[] fieldGens, ISerializerDeserializer[] fieldSerdes, int payloadSize) {
        this.size = size;
        tupleGens = new TupleGenerator[size];
        for (int i = 0; i < size; i++) {
            tupleGens[i] = new TupleGenerator(fieldGens, fieldSerdes, payloadSize);
        }
    }

    public void generate() throws IOException {
        for(TupleGenerator tupleGen : tupleGens) {
            tupleGen.next();
        }
    }

    public int size() {
        return size;
    }

    public ITupleReference get(int ix) {
        return tupleGens[ix].get();
    }
}
