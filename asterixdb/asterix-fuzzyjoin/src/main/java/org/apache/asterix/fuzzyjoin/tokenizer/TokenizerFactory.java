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

public class TokenizerFactory {
    public enum TokenizerType {
        NGram("NGram"),
        Word("Word");

        private String value;
        TokenizerType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static TokenizerType fromValue(String value) {
            return Enum.valueOf(TokenizerType.class, value);
        }
    }

    public static Tokenizer getTokenizer(String tokenizerStr, String wordSeparator, char tokenSeparator) {
        if (TokenizerType.fromValue(tokenizerStr) == TokenizerType.NGram) {
            return new NGramTokenizer();
        } else if (TokenizerType.fromValue(tokenizerStr) == TokenizerType.Word) {
            return new WordTokenizer(wordSeparator, tokenSeparator);
        }
        throw new RuntimeException("Unknown tokenizer \"" + tokenizerStr + "\".");
    }
}
