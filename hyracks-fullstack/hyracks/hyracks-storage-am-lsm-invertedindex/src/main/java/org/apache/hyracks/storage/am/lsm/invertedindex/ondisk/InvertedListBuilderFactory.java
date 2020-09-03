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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.fixedsize.FixedSizeElementInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.variablesize.VariableSizeElementInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

public class InvertedListBuilderFactory implements IInvertedListBuilderFactory {

    protected final ITypeTraits[] invListFields;
    private final boolean isFixedSize;

    public InvertedListBuilderFactory(ITypeTraits[] invListFields) {
        this.invListFields = invListFields;

        isFixedSize = InvertedIndexUtils.checkTypeTraitsAllFixed(invListFields);
    }

    @Override
    public IInvertedListBuilder create() {
        if (isFixedSize) {
            return new FixedSizeElementInvertedListBuilder(invListFields);
        } else {
            return new VariableSizeElementInvertedListBuilder(invListFields);
        }
    }
}
