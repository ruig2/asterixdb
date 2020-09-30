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
                // Currently, the tokenizer will be set later after the analyzer created
                // This is because the tokenizer logic is complex,
                // and we are already using a dedicated tokenizer factory to create tokenizer.
                // One tricky part of tokenizer is that it can be call-site specific, e.g. the string in some call-site
                // has the ATypeTag.String in the beginning of its byte array, and some doesn't,
                // so if we only know the category of the tokenizer, e.g. a WORD tokenizer,
                // we still cannot create a suitable tokenizer here as the tokenizer factory does.
                //
                // Finally we should get rid of the dedicated tokenizer factory and put its related logic
                // in the full-text descriptor and analyzer
                this.tokenizer = null;
                break;
            case NGRAM:
                throw new NotImplementedException();
            default:
                throw new InvalidParameterException();
        }
    }

}
