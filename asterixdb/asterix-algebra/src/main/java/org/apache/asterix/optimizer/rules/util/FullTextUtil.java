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
package org.apache.asterix.optimizer.rules.util;

import java.util.List;

import org.apache.asterix.metadata.entities.fulltextentity.FullTextConfig;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.IOptimizableFuncExpr;
import org.apache.asterix.runtime.evaluators.common.FullTextContainsDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FullTextUtil {
    public static boolean isFullTextFunctionExpr(IOptimizableFuncExpr expr) {
        return isFullTextFunctionExpr(expr.getFuncExpr());
    }

    public static boolean isFullTextFunctionExpr(AbstractFunctionCallExpression expr) {
        FunctionIdentifier funcId = expr.getFunctionIdentifier();
        if (funcId == BuiltinFunctions.FULLTEXT_CONTAINS || funcId == BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION) {
            return true;
        }
        return false;
    }

    // If not a full-text function expression, then return null
    // Otherwise, return the full-text config if one exists in the expression, or return the default config
    public static String getFullTextConfigNameFromExpr(IOptimizableFuncExpr expr) {
        return getFullTextConfigNameFromExpr(expr.getFuncExpr());
    }

    public static String getFullTextConfigNameFromExpr(AbstractFunctionCallExpression funcExpr) {
        if (isFullTextFunctionExpr(funcExpr) == false) {
            return null;
        }

        // ToDo: wrap the expressions in a ftcontains() function into a dedicated Java object
        // so that we don't cast the types many times
        String configName = FullTextConfig.DefaultFullTextConfig.getName();
        List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();
        for (int i = 0; i < arguments.size() - 1; i++) {
            String optionName = "";
            try {
                ConstantExpression ce = (ConstantExpression) arguments.get(i).getValue();
                optionName = ((AString) ((AsterixConstantValue) (ce.getValue())).getObject()).getStringValue();
            } catch (Exception e) {
                continue;
            }

            if (optionName.equalsIgnoreCase(FullTextContainsDescriptor.FULLTEXT_CONFIG_OPTION)) {
                ConstantExpression nextCe = (ConstantExpression) arguments.get(i + 1).getValue();
                configName = ((AString) ((AsterixConstantValue) (nextCe.getValue())).getObject()).getStringValue();
                break;
            }
        }

        return configName;
    }
}
