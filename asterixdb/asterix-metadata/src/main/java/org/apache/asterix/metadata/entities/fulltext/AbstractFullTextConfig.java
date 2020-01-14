/**
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

package org.apache.asterix.metadata.entities.fulltext;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.asterix.metadata.api.IFullTextConfig;
import org.apache.asterix.metadata.api.IFullTextFilter;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.entities.Index;

import com.google.common.collect.ImmutableList;
import org.apache.asterix.metadata.entitytupletranslators.FulltextEntityTupleTranslator;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public abstract class AbstractFullTextConfig implements IFullTextConfig {
    private final String name;
    private final Tokenizer tokenizer;
    private ImmutableList<IFullTextFilter> filters;
    private List<Index> usedByIndices;

    protected AbstractFullTextConfig(String name, Tokenizer tokenizer, ImmutableList<IFullTextFilter> filters) {
        this.name = name;
        this.tokenizer = tokenizer;
        this.filters = filters;
        this.usedByIndices = new ArrayList<>();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FullTextEntityCategory getCategory() {
        return FullTextEntityCategory.CONFIG;
    }

    @Override
    public Tokenizer getTokenizer() {
        return tokenizer;
    }

    @Override
    public List<IFullTextFilter> getFilters() {
        return filters;
    }

    @Override
    public List<Index> getUsedByIndices() {
        return usedByIndices;
    }
}
