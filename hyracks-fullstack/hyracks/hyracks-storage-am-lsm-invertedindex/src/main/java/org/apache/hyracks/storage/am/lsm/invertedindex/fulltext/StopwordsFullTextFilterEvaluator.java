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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class StopwordsFullTextFilterEvaluator extends AbstractFullTextFilterEvaluator {
    private static final long serialVersionUID = 1L;

    private final ImmutableList<String> stopwordList;

    public StopwordsFullTextFilterEvaluator(String name, ImmutableList<String> stopwordList) {
        super(name, FullTextFilterType.STOPWORDS);
        this.stopwordList = stopwordList;
    }

    public List<String> getStopwordList() {
        return stopwordList;
    }

    @Override
    public IToken processToken(TokenizerInfo.TokenizerType tokenizerType, IToken token) {
        int start = token.getStartOffset();
        int length = token.getTokenLength();

        // The List tokenizer returns token starting with the token length,
        // e.g. 8database where the byte of value 8 means the token has a length of 8
        // We need to skip the length to fetch the pure string (e.g. "database" without 8)
        if (tokenizerType == TokenizerInfo.TokenizerType.LIST) {
            int numBytesToStoreLength = UTF8StringUtil
                    .getNumBytesToStoreLength(UTF8StringUtil.getUTFLength(token.getData(), token.getStartOffset()));
            start += numBytesToStoreLength;
            length -= numBytesToStoreLength;
        }

        String str = UTF8StringUtil.getUTF8StringInArray(token.getData(), start, length);
        if (stopwordList.contains(str)) {
            return null;
        }

        return token;
    }

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String STOPWORDS_FILTER_NAME = "stopwordsFilterName";
    private static final String STOPWORDS_LIST = "stopwordsList";

    // ToDo: extract the common logics to a dedicated helper or utilization class after more filters are implemented
    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), this.serialVersionUID);
        json.put(STOPWORDS_FILTER_NAME, name);

        ArrayNode stopwordsArrayNode = OBJECT_MAPPER.createArrayNode();
        for (String s : stopwordList) {
            stopwordsArrayNode.add(s);
        }
        json.set(STOPWORDS_LIST, stopwordsArrayNode);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String name = json.get(STOPWORDS_FILTER_NAME).asText();

        // ToDo: create a new function to extract a list from json
        ImmutableList.Builder<String> stopwordsBuilder = ImmutableList.<String> builder();
        JsonNode stopwordsArrayNode = json.get(STOPWORDS_LIST);
        for (int i = 0; i < stopwordsArrayNode.size(); i++) {
            stopwordsBuilder.add(stopwordsArrayNode.get(i).asText());
        }
        ImmutableList<String> stopwords = stopwordsBuilder.build();

        return new StopwordsFullTextFilterEvaluator(name, stopwords);
    }
}
