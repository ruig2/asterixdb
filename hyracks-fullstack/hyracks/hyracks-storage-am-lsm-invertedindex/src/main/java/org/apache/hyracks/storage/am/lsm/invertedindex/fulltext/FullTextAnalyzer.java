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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class FullTextAnalyzer extends AbstractFullTextAnalyzer {
    private static final long serialVersionUID = 1L;

    public FullTextAnalyzer(IFullTextConfig.TokenizerCategory tokenizerCategory,
            ImmutableList<IFullTextFilter> filters) {
        this.filters = filters;

        switch (tokenizerCategory) {
            case WORD:
                // Similar to aqlStringTokenizerFactory which is in the upper Asterix layer
                // ToDo: should we move aqlStringTokenizerFactory so that it can be called in the Hyracks layer?
                // If so, we need to move ATypeTag to Hyracks as well
                // Another way to do so is to pass the tokenizer instance instead of the tokenizer category from Asterix to Hyracks
                // However, this may make the serializing part tricky because only the tokenizer category will be written to disk
                this.tokenizer =
                        new DelimitedUTF8StringBinaryTokenizerFactory(true, true, new UTF8WordTokenFactory((byte) 13, // ATypeTag.SERIALIZED_STRING_TYPE_TAG
                                (byte) 3) // ATypeTag.SERIALIZED_INT32_TYPE_TAG
                        ).createTokenizer();
                break;
            case NGRAM:
                throw new NotImplementedException();
            default:
                throw new InvalidParameterException();
        }
    }

    // ToDo: similar to tokenizerCategory, pass a few descriptors of the filters instead of the entire filters when constructing an analyzer
    // to avoid serializing and passing the filters from compile-time nodes to run-time nodes
    // The idea is similar to the descriptor and evaluator of a SQLPP built-in function: descriptor is used at compile-time,
    // and evaluator is used in run-time, and the descriptor contains enough information for the evaluator to run
    public FullTextAnalyzer(IFullTextConfig config) {
        this(config.getTokenizerCategory(), config.getFilters());
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("tokenizerCategory", tokenizer.getTokenizerCategory().toString());

        final ArrayNode filterArray = OBJECT_MAPPER.createArrayNode();
        for (IFullTextFilter filter : filters) {
            filterArray.add(filter.toJson(registry));
        }
        json.set("filters", filterArray);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String tokenizerCategoryStr = json.get("tokenizerCategory").asText();
        IFullTextConfig.TokenizerCategory tc =
                IFullTextConfig.TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        ArrayNode filtersJsonNode = (ArrayNode) json.get("filters");
        List<IFullTextFilter> filterList = new ArrayList<>();
        for (int i = 0; i < filtersJsonNode.size(); i++) {
            filterList.add((IFullTextFilter) registry.deserialize(filtersJsonNode.get(i)));
        }
        ImmutableList<IFullTextFilter> filters = ImmutableList.copyOf(filterList);

        return new FullTextAnalyzer(new FullTextConfig(null, tc, filters));
    }

}
