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
package org.apache.asterix.lang.common.util;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfig;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilter;

public class FullTextUtil {

    private FullTextUtil() {
    }

    //--------------------------------------- Full-text config --------------------------------------//

    // Example of full-text config create statement
    // CREATE FULLTEXT CONFIG my_second_stopword_config IF NOT EXISTS AS {
    //     "Tokenizer": "Word", // built-in tokenizers: "Word" or "NGram"
    //     "FilterPipeline": ["my_second_stopword_filter"]
    // };
    private static ARecordType getFullTextConfigRecordType() {
        final String[] fieldNames =
                { IFullTextConfig.FIELD_NAME_TOKENIZER, IFullTextConfig.FIELD_NAME_FILTER_PIPELINE };
        final IAType[] fieldTypes = { BuiltinType.ASTRING, new AOrderedListType(BuiltinType.ASTRING, null) };
        return new ARecordType("fullTextConfigRecordType", fieldNames, fieldTypes, false);
    }

    private static final ARecordType FULL_TEXT_CONFIG_RECORD_TYPE = getFullTextConfigRecordType();

    public static AdmObjectNode validateAndGetConfigNode(RecordConstructor recordConstructor)
            throws CompilationException {
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(recordConstructor);
        validator.validateType(FULL_TEXT_CONFIG_RECORD_TYPE, node);
        return node;
    }

    //--------------------------------------- Full-text filter --------------------------------------//

    // Example of full-text filter create statement
    // Note that only the type field is a must, and other fields is filter-type-specific
    //
    // CREATE FULLTEXT FILTER my_stopword_filter IF NOT EXISTS AS {
    //     "Type": "stopwords",
    //     "StopwordsList": ["xxx", "yyy", "zzz"]
    // };
    private static ARecordType getFullTextFilterRecordType() {
        final String[] fieldNames = { IFullTextFilter.FIELD_NAME_TYPE };
        final IAType[] fieldTypes = { BuiltinType.ASTRING };
        return new ARecordType("fullTextFilterRecordType", fieldNames, fieldTypes, true);
    }

    private static final ARecordType FULL_TEXT_FILTER_RECORD_TYPE = getFullTextFilterRecordType();

    public static AdmObjectNode getFilterNode(RecordConstructor recordConstructor)
            throws CompilationException {
        // Skip validation here because the current validator only supports CLOSED record validate
        // while the FULL_TEXT_FILTER_RECORD_TYPE is open and specific to the filter types,
        // e.g. stopwords filter and stemmer filter may have different fields
        final AdmObjectNode node = ExpressionUtils.toNode(recordConstructor);
        return node;
    }

}
