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

public interface IFullTextEntity {
    enum FulltextEntityCategory {
        // in progress...
        // How to show the enum name as a string in the result of a SQLPP query?
        // Current it is showned as a byte in the SQLPP terminal
        FULLTEXT_FILTER((byte) 0),
        FULLTEXT_CONFIG((byte) 1);

        private byte id;

        FulltextEntityCategory(byte id) {
            this.id = id;
        }

        // How to improve this part?
        public static FulltextEntityCategory fromId(byte id) {
            switch (id) {
                case 0:
                    return FULLTEXT_FILTER;
                case 1:
                    return FULLTEXT_CONFIG;
                default:
                    throw new OutOfRangeException(id, 0, 1);
            }
        }

        public byte getId() {
            return this.id;
        }
    }

    FulltextEntityCategory getCategory();

    String getName();
}
