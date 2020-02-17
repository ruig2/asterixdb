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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class FullTextConfig extends AbstractFullTextConfig {
    private static final long serialVersionUID = 1L;

    public FullTextConfig(String name, TokenizerCategory tokenizerCategory, ImmutableList<IFullTextFilter> filters) {
        super(name, tokenizerCategory, filters);
    }

    public FullTextConfig(String name, TokenizerCategory tokenizerCategory, ImmutableList<IFullTextFilter> filters,
            List<String> usedByIndices) {
        super(name, tokenizerCategory, filters, usedByIndices);
    }

    @Override
    // ToDo: use the tokenizer inside
    public List<String> proceedTokens(List<String> tokens) {
        List<String> results = new ArrayList<>(tokens);
        for (IFullTextFilter filter : filters) {
            results = filter.proceedTokens(results);
        }

        return results;
    }

    // This built-in default one will be used when no full-text config is specified by the user
    public static FullTextConfig DefaultFullTextConfig =
            new FullTextConfig("DEFAULT_FULL_TEXT_CONFIG", TokenizerCategory.WORD, ImmutableList.of());

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("name", name);
        json.put("tokenizerCategory", tokenizerCategory.toString());

        final ArrayNode filterArray = OBJECT_MAPPER.createArrayNode();
        for (IFullTextFilter filter : filters) {
            filterArray.add(filter.toJson(registry));
        }
        json.set("filters", filterArray);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String name = json.get("name").asText();
        final String tokenizerCategoryStr = json.get("tokenizerCategory").asText();
        TokenizerCategory tc = TokenizerCategory.fromString(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get("filters");
        List<IFullTextFilter> filterList = new ArrayList<>();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filterList.add((IFullTextFilter) registry.deserialize(filtersJsonNode.get(i)));
        }
        ImmutableList<IFullTextFilter> filters = ImmutableList.copyOf(filterList);

        return new FullTextConfig(name, tc, filters);
    }
}
