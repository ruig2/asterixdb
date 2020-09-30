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

import java.security.InvalidParameterException;

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;

import com.google.common.collect.ImmutableList;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// FullTextAnalyzer is a run-time analyzer while the IFullTextConfigDescriptor is a compile-time descriptor
//
// The descriptor is responsible for serialization and Metadata translator (i.e. be written to the metadata catalog)
// And the analyzer is to process the tokens in each NC at run-time
public class FullTextAnalyzer extends AbstractFullTextAnalyzer {
    private static final long serialVersionUID = 1L;

    public FullTextAnalyzer(IFullTextConfigDescriptor configDescriptor) {
        IFullTextConfig.TokenizerCategory tokenizerCategory = configDescriptor.getTokenizerCategory();
        ImmutableList<IFullTextFilterDescriptor> filterDescriptors = configDescriptor.getFilterDescriptors();

        this.filterDescriptors = filterDescriptors;

        ImmutableList.Builder filtersBuilder = ImmutableList.<IFullTextFilter> builder();
        for (IFullTextFilterDescriptor d : filterDescriptors) {
            filtersBuilder.add(d.getEntity());
        }
        this.filters = filtersBuilder.build();

        switch (tokenizerCategory) {
            case WORD:
                // Similar to aqlStringTokenizerFactory which is in the upper Asterix layer
                // ToDo: should we move aqlStringTokenizerFactory so that it can be called in the Hyracks layer?
                // If so, we need to move ATypeTag to Hyracks as well
                // Another way to do so is to pass the tokenizer instance instead of the tokenizer category from Asterix to Hyracks
                // However, this may make the serializing part tricky because only the tokenizer category will be written to disk
                this.tokenizer =
                        new DelimitedUTF8StringBinaryTokenizerFactory(true, true, new UTF8WordTokenFactory((byte) 13, // ATypeTag.SERIALIZED_STRING_TYPE_TAG
                                (byte) 3) // ATypeTag.SERIALIZED_INT32_TYPE_TAG
                        ).createTokenizer();
                break;
            case NGRAM:
                throw new NotImplementedException();
            default:
                throw new InvalidParameterException();
        }
    }

}
