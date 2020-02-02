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

package org.apache.asterix.metadata.entities.fulltextentity;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.api.IFullTextFilter;

import com.google.common.collect.ImmutableList;

public class FullTextConfig extends AbstractFullTextConfig {
    public FullTextConfig(String name, TokenizerCategory tokenizerCategory, ImmutableList<IFullTextFilter> filters) {
        super(name, tokenizerCategory, filters);
    }

    @Override
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
}
