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

package org.apache.asterix.metadata.entities.fulltext;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.api.IFullTextFilter;

public abstract class AbstractFullTextFilter implements IFullTextFilter {
    protected final String name;
    protected FulltextFilterKind type;
    protected List<String> usedByFTConfigs;

    public AbstractFullTextFilter(String name, FulltextFilterKind type) {
        this.name = name;
        this.type = type;
        this.usedByFTConfigs = new ArrayList<>();
    }

    @Override
    public FulltextEntityCategory getCategory() {
        return FulltextEntityCategory.FULLTEXT_FILTER;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FulltextFilterKind getFilterKind() {
        return type;
    }

    @Override
    public List<String> getUsedByFTConfigs() {
        return usedByFTConfigs;
    }

    @Override
    public void addUsedByFTConfigs(String ftConfigName) {
        usedByFTConfigs.add(ftConfigName);
    }
}
