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
        super(name, tokenizerCategory, filters, new ArrayList<>());
    }

    // For usage in fromJson() only where usedByIndices of an existing full-text config written on disk may not be null.
    public FullTextConfig(String name, TokenizerCategory tokenizerCategory, ImmutableList<IFullTextFilter> filters,
            List<String> usedByIndices) {
        super(name, tokenizerCategory, filters, usedByIndices);
    }

    // This built-in default full-text config will be used only when no full-text config is specified by the user
    // Note that on the Asterix layer, the default config should be fetched from MetadataProvider via config name when possible
    // so that it has the latest usedByIndices field
    public static final String DEFAULT_FULL_TEXT_CONFIG_NAME = "DEFAULT_FULL_TEXT_CONFIG";

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
        TokenizerCategory tc = TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get("filters");
        List<IFullTextFilter> filterList = new ArrayList<>();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filterList.add((IFullTextFilter) registry.deserialize(filtersJsonNode.get(i)));
        }
        ImmutableList<IFullTextFilter> filters = ImmutableList.copyOf(filterList);

        return new FullTextConfig(name, tc, filters);
    }

}
