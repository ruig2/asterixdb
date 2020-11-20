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

package org.apache.asterix.runtime.fulltext;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextEntityCategory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterEvaluator;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;

import com.google.common.collect.ImmutableList;

public class FullTextConfigDescriptor implements IFullTextConfigDescriptor {
    private static final long serialVersionUID = 1L;

    private final DataverseName dataverseName;
    private final String name;
    private final TokenizerCategory tokenizerCategory;
    private ImmutableList<AbstractFullTextFilterDescriptor> filterDescriptors = null;
    private final ImmutableList<String> filterNames;

    public FullTextConfigDescriptor(DataverseName dataverseName, String name, TokenizerCategory tokenizerCategory,
            ImmutableList<String> filterNames) {
        this.dataverseName = dataverseName;
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filterNames = filterNames;
    }

    // This built-in default full-text config will be used only when no full-text config is specified by the user.
    // Note that the default ft config descriptor is not stored in metadata catalog,
    // and if we are trying to get a full-text config descriptor with a name of null or empty string,
    // the metadata manager will return this default full-text config without looking into the metadata catalog
    // In this way we avoid the edge cases to insert or delete the default config in the metadata catalog
    public static FullTextConfigDescriptor getDefaultFullTextConfig() {
        return new FullTextConfigDescriptor(null, null, TokenizerCategory.WORD, ImmutableList.of());
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IFullTextConfigEvaluatorFactory createEvaluatorFactory() {
        ImmutableList.Builder<IFullTextFilterEvaluator> filtersBuilder = new ImmutableList.Builder<>();
        for (IFullTextFilterDescriptor filterDescriptor : filterDescriptors) {
            filtersBuilder.add(filterDescriptor.createEvaluator());
        }

        return new FullTextConfigEvaluatorFactory(name, tokenizerCategory, filtersBuilder.build());
    }

    @Override
    public FullTextEntityCategory getCategory() {
        return FullTextEntityCategory.CONFIG;
    }

    @Override
    public TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    public void setFilterDescriptors(ImmutableList<AbstractFullTextFilterDescriptor> filterDescriptors) {
        if (this.filterDescriptors == null) {
            this.filterDescriptors = filterDescriptors;
        }
    }

    @Override
    public ImmutableList<String> getFilterNames() {
        return filterNames;
    }
}
