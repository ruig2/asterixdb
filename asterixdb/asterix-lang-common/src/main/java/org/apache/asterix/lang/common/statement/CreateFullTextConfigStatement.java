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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfig;

import java.util.ArrayList;
import java.util.List;

public class CreateFullTextConfigStatement extends AbstractStatement {
    /* Example of SQLPP DDL to create config:
    CREATE FULLTEXT CONFIG my_first_stopword_config IF NOT EXISTS AS {
        "Tokenizer": "Word", // built-in tokenizers: "Word" or "NGram"
        "FilterPipeline": ["my_first_stopword_filter"]
    };
     */

    private DataverseName dataverseName;
    private String configName;
    private boolean ifNotExists;
    private RecordConstructor expr;

    public CreateFullTextConfigStatement(DataverseName dataverseName, String configName, boolean ifNotExists,
            RecordConstructor expr) {
        this.dataverseName = dataverseName;
        this.configName = configName;
        this.ifNotExists = ifNotExists;
        this.expr = expr;
    }

    public static void checkExpression(Statement stmt) throws Exception {
        // Do nothing for now
        return;
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
        return Kind.CREATE_FULLTEXT_CONFIG;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public Expression getExpression() {
        return expr;
    }

    public IFullTextConfig.TokenizerCategory getTokenizerCategory() throws AlgebricksException {
        List<FieldBinding> fb = getFields();
        String tokenizerTupleKeyStr =
                ((LiteralExpr) (fb.get(0).getLeftExpr())).getValue().getStringValue().toLowerCase();
        if (tokenizerTupleKeyStr.equalsIgnoreCase(IFullTextConfig.FIELD_NAME_TOKENIZER) == false) {
            throw CompilationException.create(ErrorCode.COMPILATION_INVALID_EXPRESSION,
                    "expect tokenizer in the first row");
        }
        String tokenizerTupleValueStr =
                ((LiteralExpr) (fb.get(0).getRightExpr())).getValue().getStringValue().toLowerCase();
        IFullTextConfig.TokenizerCategory tokenizerCategory =
                IFullTextConfig.TokenizerCategory.getEnumIgnoreCase(tokenizerTupleValueStr);

        return tokenizerCategory;
    }

    public List<String> getFilterNames() throws AlgebricksException {
        List<FieldBinding> fb = getFields();
        String filterPipelineTupleKeyStr =
                ((LiteralExpr) (fb.get(1).getLeftExpr())).getValue().getStringValue().toLowerCase();
        if (filterPipelineTupleKeyStr.equalsIgnoreCase(IFullTextConfig.FIELD_NAME_FILTER_PIPELINE) == false) {
            throw CompilationException.create(ErrorCode.COMPILATION_INVALID_EXPRESSION,
                    "expect filter pipeline in the second row");
        }
        List<String> filterNames = new ArrayList<>();
        for (Expression l : ((ListConstructor) (fb.get(1).getRightExpr())).getExprList()) {
            filterNames.add(((LiteralExpr) l).getValue().getStringValue());
        }

        return filterNames;
    }

    private List<FieldBinding> getFields() throws AlgebricksException {
        RecordConstructor rc = expr;
        List<FieldBinding> fb = rc.getFbList();
        if (fb.size() < 2) {
            throw CompilationException.create(ErrorCode.COMPILATION_INVALID_EXPRESSION,
                    "number of parameter less than expected");
        }
        return fb;
    }

}
