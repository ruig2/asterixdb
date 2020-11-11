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

import java.io.Serializable;

import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo;

// The full-text filter evaluator needs to be stored in the index local resource, so it needs to be IJsonSerializable
// Also, it needs to be distributed from CC (compile-time) to NC (run-time), so it needs to be Serializable
//
// For the full-text config evaluator, we distribute and store the config evaluator factory in the index local resource instead.
// So the config evaluator IFullTextConfigEvaluator is not IJsonSerializable nor Serializable
public interface IFullTextFilterEvaluator extends IFullTextEntityEvaluator, IJsonSerializable, Serializable {
    FullTextFilterType getFilterType();

    IToken processToken(TokenizerInfo.TokenizerType tokenizerType, IToken token);
}
