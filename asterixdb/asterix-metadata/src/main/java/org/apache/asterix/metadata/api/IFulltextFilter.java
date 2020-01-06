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

package org.apache.asterix.metadata.api;

import org.apache.commons.math3.exception.OutOfRangeException;

import java.util.List;

// in progress...

public interface IFulltextFilter extends IFulltextEntity {

    enum FulltextFilterKind {
        // Assume the number of filter types are less than 2^8 = 256
        // When serializing the filter, only 8 bits will be reserved for the filter type
        // And don't change the existing value of the enums because this may corrupt the programs with older versions
        STOPWORD((byte) 0),
        SYNONYM((byte) 1);

        private final byte id;

        FulltextFilterKind(byte id) {
            this.id = id;
        }

        // How to improve this part?
        public static FulltextFilterKind fromId(byte id) {
            switch (id) {
                case 0:
                    return STOPWORD;
                case 1:
                    return SYNONYM;
                default:
                    throw new OutOfRangeException(id, 0, 1);
            }
        }

        public byte getId() {
            return this.id;
        }
    }

    FulltextFilterKind getFilterKind();

    List<String> getUsedByFTConfigs();

    void addUsedByFTConfigs(String ftConfigName);
}
