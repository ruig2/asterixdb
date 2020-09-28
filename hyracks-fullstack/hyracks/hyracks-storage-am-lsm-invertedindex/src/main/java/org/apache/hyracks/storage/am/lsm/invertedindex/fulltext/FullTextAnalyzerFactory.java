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

package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

public class FullTextAnalyzerFactory implements IFullTextAnalyzerFactory {
    private static final long serialVersionUID = 1L;

    private final IFullTextConfigDescriptor configDescriptor;

    /*
    public FullTextAnalyzerFactory(IFullTextAnalyzer analyzer) {
        this.analyzer = analyzer;
    }
     */
    public FullTextAnalyzerFactory(IFullTextConfigDescriptor configDescriptor) {
        this.configDescriptor = configDescriptor;
    }

    @Override
    public IFullTextAnalyzer createFullTextAnalyzer() {
        return new FullTextAnalyzer(configDescriptor);
        /*
        if (analyzer == null) {
            // If not specified, use the the default full-text config
            // Note that though the tokenizer here is of category Word, it may be replaced by a NGram tokenizer at run time
            //     for NGram index.
            return new FullTextAnalyzer(IFullTextConfig.TokenizerCategory.WORD, ImmutableList.of());
        }
        
        // All the components in the full-text config can be reused except the tokenizer.
        // The same config may be used in different places at the same time
        // For example, in ftcontains() the left expression and right expression need to be proceeded by two full-text configs
        // with the same filters but dedicated tokenizers
        return new FullTextAnalyzer(analyzer.getTokenizer().getTokenizerCategory(), analyzer.getFilters());
        
         */
    }

    /*
    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        // ToDo: add tokenizerFactory into FullTextConfigFactory so a new tokenizer can be generated on-the-fly
        // rather than pass a tokenizer from the upper-layer caller to the full-text config
        if (analyzer != null) {
            json.set("fullTextAnalyzer", analyzer.toJson(registry));
        } else {
            json.set("fullTextAnalyzer", null);
        }
        return json;
    }
    
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        if (json.get("fullTextAnalyzer").isNull()) {
            return null;
        }
    
        final IFullTextAnalyzer analyzer = (IFullTextAnalyzer) registry.deserialize(json.get("fullTextAnalyzer"));
        return new FullTextAnalyzerFactory(analyzer);
    }
     */

}
