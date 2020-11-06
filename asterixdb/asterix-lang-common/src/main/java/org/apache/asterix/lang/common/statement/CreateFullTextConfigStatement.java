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
package org.apache.asterix.lang.common.statement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.FullTextUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfig;

public class CreateFullTextConfigStatement extends AbstractStatement {

    private final DataverseName dataverseName;
    private final String configName;
    private final boolean ifNotExists;
    private final AdmObjectNode configNode;

    public CreateFullTextConfigStatement(DataverseName dataverseName, String configName, boolean ifNotExists,
            RecordConstructor expr) throws CompilationException {
        this.dataverseName = dataverseName;
        this.configName = configName;
        this.ifNotExists = ifNotExists;
        this.configNode = FullTextUtil.validateAndGetConfigNode(expr);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getConfigName() {
        return configName;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FULL_TEXT_CONFIG;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public IFullTextConfig.TokenizerCategory getTokenizerCategory() throws HyracksDataException {
        String tokenizerCategoryStr = configNode.getString(IFullTextConfig.FIELD_NAME_TOKENIZER);
        IFullTextConfig.TokenizerCategory tokenizerCategory =
                IFullTextConfig.TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);

        return tokenizerCategory;
    }

    public List<String> getFilterNames() throws AlgebricksException {
        AdmArrayNode arrayNode = (AdmArrayNode) configNode.get(IFullTextConfig.FIELD_NAME_FILTER_PIPELINE);
        List<String> results = new ArrayList<>();

        Iterator<IAdmNode> iterator = arrayNode.iterator();
        while (iterator.hasNext()) {
            results.add(((AdmStringNode) iterator.next()).get());
        }

        return results;
    }

}
