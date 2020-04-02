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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.util.string.UTF8StringUtil;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class StopwordsFullTextFilter extends AbstractFullTextFilter {
    private static final long serialVersionUID = 1L;

    ImmutableList<String> stopwordList;

    public StopwordsFullTextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, IFullTextFilter.FullTextFilterType.STOPWORDS);
        this.stopwordList = stopwordList;
    }

    public List<String> getStopwordList() {
        return stopwordList;
    }

    @Override
    public List<String> proceedTokens(List<String> tokens) {
        List<String> result = new ArrayList<>();

        for (String s : tokens) {
            if (stopwordList.contains(s) == false) {
                result.add(s);
            }
        }
        return result;
    }

    @Override public IToken processToken(IToken token) {
        // ToDo: in progress...
        /*
        String str = UTF8StringUtil.toString(token.getData(), token.getStartOffset());
        System.out.print(str + " ");
        if (stopwordList.contains(str)) {
            System.out.println("contains");
            //return null;
        }
        System.out.println();
         */
        return token;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("stopwordsFilterName", name);

        ArrayNode stopwordsArrayNode = AbstractFullTextConfig.OBJECT_MAPPER.createArrayNode();
        for (String s : stopwordList) {
            stopwordsArrayNode.add(s);
        }
        json.set("stopwordsList", stopwordsArrayNode);

        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final String name = json.get("stopwordsFilterName").asText();

        ImmutableList.Builder<String> stopwordsBuilder = ImmutableList.<String> builder();
        JsonNode stopwordsArrayNode = json.get("stopwordsList");
        for (int i = 0; i < stopwordsArrayNode.size(); i++) {
            stopwordsBuilder.add(stopwordsArrayNode.get(i).asText());
        }
        ImmutableList stopwords = stopwordsBuilder.build();

        return new StopwordsFullTextFilter(name, stopwords);
    }
}
