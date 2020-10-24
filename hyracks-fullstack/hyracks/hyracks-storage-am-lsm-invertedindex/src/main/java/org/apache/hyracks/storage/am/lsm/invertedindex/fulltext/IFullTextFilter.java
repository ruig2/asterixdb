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

import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;

public interface IFullTextFilter extends IFullTextEntity {
    // case-insensitive
    String FIELD_NAME_TYPE = "type";
    String FIELD_NAME_STOPWORDS = "stopwords";
    String FIELD_NAME_STOPWORDS_LIST = "stopwordsList";
    String FIELD_NAME_STEMMER = "stemmer";
    String FIELD_NAME_STEMMER_LANGUAGE = "language";

    enum FullTextFilterType {
        STOPWORDS("Stopwords"),
        SYNONYM("Synonym"),
        STEMMER("Stemmer");

        private String value;

        FullTextFilterType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static FullTextFilterType getEnumIgnoreCase(String str) {
            FullTextFilterType type = EnumUtils.getEnumIgnoreCase(FullTextFilterType.class, str);

            if (type == null) {
                throw new IllegalArgumentException("Cannot convert string " + str + " to FullTextFilterType!");
            }
            return type;
        }
    }

    FullTextFilterType getFilterType();

    List<String> getUsedByFTConfigs();

    void addUsedByFTConfigs(String ftConfigName);

    IToken processToken(TokenizerInfo.TokenizerType tokenizerType, IToken token);
}
