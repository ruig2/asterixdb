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

package org.apache.asterix.fuzzyjoin.fulltextentity;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;

import com.google.common.collect.ImmutableList;

public abstract class AbstractFullTextConfig implements IFullTextConfig {
    private final String name;
    private final TokenizerCategory tokenizerCategory;
    private ImmutableList<IFullTextFilter> filters;
    // in progress... maybe use Index class instead of String?
    private List<String> usedByIndices;

    protected AbstractFullTextConfig(String name, TokenizerCategory tokenizerCategory, ImmutableList<IFullTextFilter> filters) {
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
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
    public TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    @Override
    public List<IFullTextFilter> getFilters() {
        return filters;
    }

    @Override
    public List<String> getUsedByIndices() {
        return usedByIndices;
    }
}
