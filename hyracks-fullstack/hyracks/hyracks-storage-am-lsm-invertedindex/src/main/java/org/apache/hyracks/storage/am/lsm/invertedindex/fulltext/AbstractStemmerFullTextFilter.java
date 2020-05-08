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

import java.io.IOException;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordToken;
import org.apache.hyracks.util.string.UTF8StringUtil;

import opennlp.tools.stemmer.snowball.SnowballStemmer;

public abstract class AbstractStemmerFullTextFilter extends AbstractFullTextFilter {
    private static final long serialVersionUID = 1L;
    protected SnowballStemmer stemmer;

    public enum StemmerLanguage {
        ENGLISH;

        public static StemmerLanguage getEnumIgnoreCase(String str) {
            StemmerLanguage language = EnumUtils.getEnumIgnoreCase(StemmerLanguage.class, str);
            if (language == null) {
                throw new IllegalArgumentException("Cannot convert the string " + str + " to StemmerLanguage type!");
            }

            return language;
        }
    }

    protected StemmerLanguage language;

    protected AbstractStemmerFullTextFilter(String name) {
        super(name, FullTextFilterType.STEMMER);
    }

    public StemmerLanguage getLanguage() {
        return language;
    }

    // This should be put into FullTextUtil class, however it is in asterix-layer and its usage is limited
    public static AbstractStemmerFullTextFilter createStemmerFullTextFilter(String name, String languageStr) {
        StemmerLanguage language = StemmerLanguage.getEnumIgnoreCase(languageStr);
        switch (language) {
            case ENGLISH:
                return new EnglishStemmerFullTextFilter(name);
            default:
                throw new IllegalArgumentException("The stemmer language not supported!");
        }
    }

    @Override
    public IToken processToken(TokenizerInfo.TokenizerType tokenizerType, IToken token) {
        int start = token.getStartOffset();
        int length = token.getTokenLength();

        // The List tokenizer returns token starting with the token length,
        // e.g. 8database where the byte of value 8 means the token has a length of 8
        // We need to skip the length to fetch the pure string (e.g. "database" without 8)
        if (tokenizerType == TokenizerInfo.TokenizerType.LIST) {
            int numBytesToStoreLength = UTF8StringUtil
                    .getNumBytesToStoreLength(UTF8StringUtil.getUTFLength(token.getData(), token.getStartOffset()));
            start += numBytesToStoreLength;
            length -= numBytesToStoreLength;
        }

        String str = UTF8StringUtil.getUTF8StringInArray(token.getData(), start, length);
        // Will stemmed be null?
        String stemmedStr = String.valueOf(stemmer.stem(str));

        System.out.println("before: \t" + str);
        System.out.println("after:  \t" + stemmedStr);
        System.out.println();

        if (stemmedStr.equals(str)) {
            return token;
        } else {
            // ToDo: move the aqlStringTokenizerFactory from Asterix-layer to Hyracks-layer so that we can use it here
            IToken stemmedToken = new UTF8WordToken(
                    (byte) 13, // ATypeTag.SERIALIZED_STRING_TYPE_TAG
                    (byte) 3 // ATypeTag.SERIALIZED_INT32_TYPE_TAG
            );

            UTF8StringBuilder stringBuilder = new UTF8StringBuilder();
            GrowableArray array = new GrowableArray();
            try {
                stringBuilder.reset(array, stemmedStr.length());
                stringBuilder.appendString(stemmedStr);
                stringBuilder.finish();
            } catch (IOException e) {
                e.printStackTrace();
            }

            int stemmedTokenStart = 0;
            int stemmedTokenLength = array.getLength();

            //if (tokenizerType == TokenizerInfo.TokenizerType.STRING) {
                // For tokenizer of type LIST, the length of the token needs to be included in the first few bytes to be consistent
                int stemmedTokenNumBytesToStoreLength = UTF8StringUtil.getNumBytesToStoreLength(UTF8StringUtil.getUTFLength(array.getByteArray(), stemmedTokenStart));
                stemmedTokenStart += stemmedTokenNumBytesToStoreLength;
                stemmedTokenLength -= stemmedTokenNumBytesToStoreLength;
            //}

            stemmedToken.reset(array.getByteArray(), stemmedTokenStart, array.getLength(), stemmedTokenLength, 1);
            return stemmedToken;
        }

    }
}
