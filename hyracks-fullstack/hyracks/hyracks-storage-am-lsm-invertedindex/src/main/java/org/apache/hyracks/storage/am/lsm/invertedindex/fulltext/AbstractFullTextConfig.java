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

package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

import java.security.InvalidParameterException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class AbstractFullTextConfig implements IFullTextConfig {
    protected final String name;
    protected final TokenizerCategory tokenizerCategory;
    // By default, the tokenizer should be of the type DelimitedUTF8StringBinaryTokenizer
    // tokenizer needs be replaced on-the-fly when used in the ftcontains() function
    // ftcontains() can take two types of input:
    // 1) string where a default DelimitedUTF8StringBinaryTokenizer is fine,
    // and 2) a list of string as input where we may need a AUnorderedListBinaryTokenizer or AOrderedListBinaryTokenizer
    protected IBinaryTokenizer tokenizer;
    protected ImmutableList<IFullTextFilter> filters;
    protected List<String> usedByIndices;
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected AbstractFullTextConfig(String name, TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilter> filters, List<String> usedByIndices) {
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filters = filters;
        this.usedByIndices = usedByIndices;

        ITokenFactory tokenFactory = null;
        switch (tokenizerCategory) {
            case WORD:
                this.tokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                        new UTF8WordTokenFactory()).createTokenizer();
                break;
            case NGRAM:
                throw new NotImplementedException();
                //this.tokenizer = new NGramUTF8StringBinaryTokenizerFactory(gramLength, usePrePost, true, true,
                //        new UTF8NGramTokenFactory());
            default:
                throw new InvalidParameterException();
        }
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
    public ImmutableList<IFullTextFilter> getFilters() {
        return filters;
    }

    @Override
    public List<String> getUsedByIndices() {
        return usedByIndices;
    }

    @Override
    public void addUsedByIndices(String indexName) {
        this.usedByIndices.add(indexName);
    }

    @Override
    public void setTokenizer(IBinaryTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public IBinaryTokenizer getTokenizer() {
        return this.tokenizer;
    }
}
