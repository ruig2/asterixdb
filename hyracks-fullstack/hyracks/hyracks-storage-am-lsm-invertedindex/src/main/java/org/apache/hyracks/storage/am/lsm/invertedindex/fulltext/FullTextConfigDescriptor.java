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

import static org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.AbstractFullTextConfig.OBJECT_MAPPER;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

public class FullTextConfigDescriptor implements IFullTextConfigDescriptor {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final IFullTextConfig.TokenizerCategory tokenizerCategory;
    private final ImmutableList<IFullTextFilterDescriptor> filterDescriptors;
    protected List<String> usedByIndices;

    public FullTextConfigDescriptor(String name, IFullTextConfig.TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilterDescriptor> filterDescriptors) {
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filterDescriptors = filterDescriptors;
    }

    // For usage in fromJson() only where usedByIndices of an existing full-text config written on disk may not be null.
    public FullTextConfigDescriptor(String name, IFullTextConfig.TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilterDescriptor> filterDescriptors, List<String> usedByIndices) {
        this(name, tokenizerCategory, filterDescriptors);
        this.usedByIndices = usedByIndices;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IFullTextEntity getEntity() {
        ImmutableList.Builder<IFullTextFilter> filtersBuilder = new ImmutableList.Builder<>();
        for (IFullTextEntityDescriptor filterDescriptor : filterDescriptors) {
            filtersBuilder.add((IFullTextFilter) filterDescriptor.getEntity());
        }

        return new FullTextConfig(name, tokenizerCategory, filtersBuilder.build(), usedByIndices);
    }

    @Override
    public IFullTextEntity.FullTextEntityCategory getCategory() {
        return IFullTextEntity.FullTextEntityCategory.CONFIG;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("name", name);
        json.put("tokenizerCategory", tokenizerCategory.toString());

        final ArrayNode filterArray = OBJECT_MAPPER.createArrayNode();
        for (IFullTextEntityDescriptor filterDescriptor : filterDescriptors) {
            filterArray.add(filterDescriptor.toJson(registry));
        }
        json.set("filters", filterArray);

        ArrayNode usedByIndicesArrayNode = AbstractFullTextConfig.OBJECT_MAPPER.createArrayNode();
        for (String indexName : usedByIndices) {
            usedByIndicesArrayNode.add(indexName);
        }
        json.set("usedByIndices", usedByIndicesArrayNode);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String name = json.get("name").asText();
        final String tokenizerCategoryStr = json.get("tokenizerCategory").asText();
        IFullTextConfig.TokenizerCategory tc =
                IFullTextConfig.TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get("filters");
        List<IFullTextFilterDescriptor> filterDescriptors = new ArrayList<>();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filterDescriptors.add((IFullTextFilterDescriptor) registry.deserialize(filtersJsonNode.get(i)));
        }
        ImmutableList<IFullTextFilterDescriptor> filters = ImmutableList.copyOf(filterDescriptors);

        ImmutableList.Builder<String> usedByIndicesBuilder = ImmutableList.<String> builder();
        JsonNode usedByIndicesArrayNode = json.get("usedByIndices");
        for (int i = 0; i < usedByIndicesArrayNode.size(); i++) {
            usedByIndicesBuilder.add(usedByIndicesArrayNode.get(i).asText());
        }
        ImmutableList usedByIndices = usedByIndicesBuilder.build();

        return new FullTextConfigDescriptor(name, tc, filters, usedByIndices);
    }

    @Override
    public IFullTextConfig.TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    @Override
    public ImmutableList<IFullTextFilterDescriptor> getFilterDescriptors() {
        return filterDescriptors;
    }

    @Override
    public List<String> getUsedByIndices() {
        return usedByIndices;
    }

    @Override
    public void addUsedByIndex(String indexName) {
        this.usedByIndices.add(indexName);
    }

}
