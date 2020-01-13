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

package org.apache.asterix.fuzzyjoin.tokenizer;

import org.apache.commons.lang3.EnumUtils;

public class TokenizerFactory {
    public enum TokenizerCategory {
        NGRAM,
        WORD;

        public static TokenizerCategory fromString(String str) {
            return EnumUtils.getEnumIgnoreCase(TokenizerCategory.class, str);
        }
    }

    public static Tokenizer getTokenizer(String tokenizerStr, String wordSeparator, char tokenSeparator) {
        if (TokenizerCategory.fromString(tokenizerStr) == TokenizerCategory.NGRAM) {
            return new NGramTokenizer();
        } else if (TokenizerCategory.fromString(tokenizerStr) == TokenizerCategory.WORD) {
            return new WordTokenizer(wordSeparator, tokenSeparator);
        }
        throw new RuntimeException("Unknown tokenizer \"" + tokenizerStr + "\".");
    }
}
