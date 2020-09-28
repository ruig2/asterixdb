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

import static org.apache.hyracks.util.string.UTF8StringUtil.getUTF8StringInArray;

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.google.common.collect.ImmutableList;

public class AbstractFullTextAnalyzer implements IFullTextAnalyzer {

    protected IBinaryTokenizer tokenizer;
    protected ImmutableList<IFullTextFilterDescriptor> filterDescriptors;
    protected ImmutableList<IFullTextFilter> filters;

    private IToken currentToken = null;
    private IToken nextToken = null;

    @Override
    public IBinaryTokenizer getTokenizer() {
        return tokenizer;
    }

    @Override
    public void setTokenizer(IBinaryTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public ImmutableList<IFullTextFilterDescriptor> getFilterDescriptors() {
        return filterDescriptors;
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        currentToken = null;
        nextToken = null;
        tokenizer.reset(data, start, length);
    }

    // For debug usage
    private void printCurrentToken() {
        // ToDo: wrap the following logic into a function
        int start = currentToken.getStartOffset();
        int length = currentToken.getTokenLength();
        if (tokenizer.getTokenizerType() == TokenizerInfo.TokenizerType.LIST) {
            int numBytesToStoreLength = UTF8StringUtil.getNumBytesToStoreLength(
                    UTF8StringUtil.getUTFLength(currentToken.getData(), currentToken.getStartOffset()));
            start += numBytesToStoreLength;
            length -= numBytesToStoreLength;
        }

        String s = getUTF8StringInArray(currentToken.getData(), start, length);
        System.out.println("current token: " + s);
    }

    @Override
    public IToken getToken() {
        printCurrentToken();

        return currentToken;
    }

    @Override
    public boolean hasNext() {
        if (nextToken != null) {
            return true;
        }

        while (tokenizer.hasNext()) {
            tokenizer.next();
            IToken candidateToken = tokenizer.getToken();
            for (IFullTextFilter filter : filters) {
                // ToDo: Tokenizer of TokenizerType.List would return strings starting with the length,
                // e.g. 8database where 8 is the length
                // Should we let TokenizerType.List returns the same thing as TokenizerType.String to make things easier?
                // Otherwise, filters need tokenizer.getTokenizerType to decide if they need to remove the length themselves
                candidateToken = filter.processToken(tokenizer.getTokenizerType(), candidateToken);
                // null means the token is removed, i.e. it is a stopword
                if (candidateToken == null) {
                    break;
                }
            }

            if (candidateToken != null) {
                nextToken = candidateToken;
                break;
            }
        }

        return nextToken != null;
    }

    @Override
    public void next() {
        currentToken = nextToken;
        nextToken = null;
    }

    @Override
    public int getTokensCount() {
        return tokenizer.getTokensCount();
    }

}
