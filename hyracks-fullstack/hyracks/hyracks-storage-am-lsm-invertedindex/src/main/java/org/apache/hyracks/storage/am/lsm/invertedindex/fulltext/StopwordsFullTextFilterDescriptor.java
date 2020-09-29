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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class StopwordsFullTextFilterDescriptor extends AbstractFullTextFilterDescriptor {
    private static final long serialVersionUID = 1L;

    public ImmutableList<String> stopwordList;

    public StopwordsFullTextFilterDescriptor(String name, ImmutableList<String> stopwordList,
            List<String> usedByConfigs) {
        super(name, usedByConfigs);
        this.stopwordList = stopwordList;
    }

    @Override
    public IFullTextFilter.FullTextFilterType getFilterType() {
        return IFullTextFilter.FullTextFilterType.STOPWORDS;
    }

    public List<String> getStopwordList() {
        return this.stopwordList;
    }

    @Override
    public IFullTextEntity getEntity() {
        return new StopwordsFullTextFilter(name, stopwordList);
    }

    private static final String STOPWORDS_FILTER_NAME = "stopwordsFilterName";
    private static final String STOPWORDS_LIST = "stopwordsList";
    private static final String USED_BY_CONFIGS = "usedByConfigs";

    // ToDo: extract the common logics to a dedicated helper or utilization class after more filters are implemented
    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), this.serialVersionUID);
        json.put(STOPWORDS_FILTER_NAME, name);

        ArrayNode stopwordsArrayNode = AbstractFullTextConfig.OBJECT_MAPPER.createArrayNode();
        for (String s : stopwordList) {
            stopwordsArrayNode.add(s);
        }
        json.set(STOPWORDS_LIST, stopwordsArrayNode);

        ArrayNode usedByConfigsArrayNode = AbstractFullTextConfig.OBJECT_MAPPER.createArrayNode();
        for (String s : usedByConfigs) {
            usedByConfigsArrayNode.add(s);
        }
        json.set(USED_BY_CONFIGS, usedByConfigsArrayNode);
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

        ImmutableList.Builder<String> usedByIndicesBuilder = ImmutableList.<String> builder();
        JsonNode usedByIndicesArrayNode = json.get(USED_BY_CONFIGS);
        for (int i = 0; i < usedByIndicesArrayNode.size(); i++) {
            usedByIndicesBuilder.add(usedByIndicesArrayNode.get(i).asText());
        }
        ImmutableList<String> usedByIndices = usedByIndicesBuilder.build();

        return new StopwordsFullTextFilterDescriptor(name, stopwords, usedByIndices);
    }
}
