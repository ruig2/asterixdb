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

    private final String dataverseName;
    private final String name;
    private final IFullTextConfig.TokenizerCategory tokenizerCategory;
    private final ImmutableList<IFullTextFilterDescriptor> filterDescriptors;

    // This built-in default full-text config will be used only when no full-text config is specified by the user
    // Note that the default ft config descriptor is not stored in metadata catalog,
    // and if we are trying to get a ft config descriptor with no name or this default name,
    // the metadata manager will return a default one without looking into the metadata catalog
    // In this way we avoid the edge cases to insert or delete the default config in the catalog
    public static final String DEFAULT_FULL_TEXT_CONFIG_NAME = "DEFAULT_FULL_TEXT_CONFIG";

    public FullTextConfigDescriptor(String dataverseName, String name,
            IFullTextConfig.TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilterDescriptor> filterDescriptors) {
        this.dataverseName = dataverseName;
        this.name = name;
        this.tokenizerCategory = tokenizerCategory;
        this.filterDescriptors = filterDescriptors;
    }

    public static IFullTextConfigDescriptor getDefaultFullTextConfig() {
        return new FullTextConfigDescriptor(null, DEFAULT_FULL_TEXT_CONFIG_NAME, IFullTextConfig.TokenizerCategory.WORD,
                ImmutableList.of());
    }

    @Override
    public String getDataverseName() {
        return dataverseName;
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

        return new FullTextConfig(name, tokenizerCategory, filtersBuilder.build());
    }

    @Override
    public IFullTextEntity.FullTextEntityCategory getCategory() {
        return IFullTextEntity.FullTextEntityCategory.CONFIG;
    }

    @Override
    public IFullTextConfig.TokenizerCategory getTokenizerCategory() {
        return tokenizerCategory;
    }

    @Override
    public ImmutableList<IFullTextFilterDescriptor> getFilterDescriptors() {
        return filterDescriptors;
    }

    private static final String FIELD_DATAVERSE_NAME = "dataverseName";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TOKENIZER_CATEGORY = "tokenizerCategory";
    private static final String FIELD_FILTERS = "filters";

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put(FIELD_DATAVERSE_NAME, dataverseName);
        json.put(FIELD_NAME, name);
        json.put(FIELD_TOKENIZER_CATEGORY, tokenizerCategory.toString());

        final ArrayNode filterArray = OBJECT_MAPPER.createArrayNode();
        for (IFullTextEntityDescriptor filterDescriptor : filterDescriptors) {
            filterArray.add(filterDescriptor.toJson(registry));
        }
        json.set(FIELD_FILTERS, filterArray);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String dataverseName = json.get(FIELD_DATAVERSE_NAME).asText();
        final String name = json.get(FIELD_NAME).asText();
        final String tokenizerCategoryStr = json.get(FIELD_TOKENIZER_CATEGORY).asText();
        IFullTextConfig.TokenizerCategory tc =
                IFullTextConfig.TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get(FIELD_FILTERS);
        List<IFullTextFilterDescriptor> filterDescriptors = new ArrayList<>();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filterDescriptors.add((IFullTextFilterDescriptor) registry.deserialize(filtersJsonNode.get(i)));
        }
        ImmutableList<IFullTextFilterDescriptor> filters = ImmutableList.copyOf(filterDescriptors);

        return new FullTextConfigDescriptor(dataverseName, name, tc, filters);
    }

}
