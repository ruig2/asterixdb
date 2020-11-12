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

package org.apache.asterix.runtime.fulltext;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextEntityCategory;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextFilterType;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class AbstractFullTextFilterDescriptor implements IFullTextFilterDescriptor {
    protected final DataverseName dataverseName;
    protected final String name;

    public AbstractFullTextFilterDescriptor(DataverseName dataverseName, String name) {
        this.dataverseName = dataverseName;
        this.name = name;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FullTextFilterType getFilterType() {
        throw new NotImplementedException();
    }

    @Override
    public FullTextEntityCategory getCategory() {
        return FullTextEntityCategory.FILTER;
    }
}
