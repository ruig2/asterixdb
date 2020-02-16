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

import java.util.List;

import org.apache.commons.lang3.EnumUtils;

public interface IFullTextConfig extends IFullTextEntity {
    // case-insensitive
    String FIELD_NAME_TOKENIZER = "tokenizer";
    String FIELD_NAME_FILTER_PIPELINE = "filterpipeline";

    enum TokenizerCategory {
        NGRAM,
        WORD;

        public static TokenizerCategory fromString(String str) {
            return EnumUtils.getEnumIgnoreCase(TokenizerCategory.class, str);
        }
    }

    TokenizerCategory getTokenizerCategory();

    List<IFullTextFilter> getFilters();

    List<String> getUsedByIndices();

    void addUsedByIndices(String indexName);

    // in progress... Assume the input is tokenized already
    // ToDo: proceed in a stream way to avoid copying strings and tokens
    // A good reference is IBinaryTokenizer
    List<String> proceedTokens(List<String> tokens);
}
