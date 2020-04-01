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

import com.google.common.collect.ImmutableList;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class FullTextConfigFactory implements IFullTextConfigFactory {
    private static final long serialVersionUID = 1L;

    private final IFullTextConfig config;

    public FullTextConfigFactory(IFullTextConfig config) {
        this.config = config;
    }

    @Override
    public IFullTextConfig createFullTextConfig() {
        // All the components in the full-text config can be reused except the tokenizer.
        // The same config may be used in different places at the same time
        // For example, in ftcontains() the left expression and right expression need to be proceeded by two full-text configs
        // with the same filters but dedicated tokenizers
        return new FullTextConfig(config.getName(), config.getTokenizerCategory(),
                config.getFilters(), config.getUsedByIndices());
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        // in progress... add tokenizerFactory here so a new tokenizer can be generated
        json.set("fullTextConfig", config.toJson(registry));
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final IFullTextConfig config = (IFullTextConfig) registry.deserialize(json.get("fullTextConfig"));
        return new FullTextConfigFactory(config);
    }
}
