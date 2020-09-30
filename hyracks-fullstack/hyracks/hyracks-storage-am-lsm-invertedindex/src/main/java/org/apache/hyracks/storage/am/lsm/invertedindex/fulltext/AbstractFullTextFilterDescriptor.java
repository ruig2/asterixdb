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
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class AbstractFullTextFilterDescriptor implements IFullTextFilterDescriptor {
    protected final String name;
    protected List<String> usedByConfigs;

    public AbstractFullTextFilterDescriptor(String name, List<String> usedByConfigs) {
        this.name = name;
        this.usedByConfigs = usedByConfigs;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IFullTextFilter.FullTextFilterType getFilterType() {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getUsedByConfigs() {
        return usedByConfigs;
    }

    @Override
    public IFullTextEntity.FullTextEntityCategory getCategory() {
        return IFullTextEntity.FullTextEntityCategory.FILTER;
    }

    @Override
    public void addUsedByConfig(String usedByConfig) {
        this.usedByConfigs.add(usedByConfig);
    }

    @Override
    public List<String> deleteUsedByConfig(String usedByConfig) {
        throw new NotImplementedException();
    }

    @Override
    public IFullTextEntity getEntity() {
        throw new NotImplementedException();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        throw new NotImplementedException();
    }

}
