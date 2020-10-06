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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.am.IOptimizableFuncExpr;
import org.apache.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import org.apache.asterix.runtime.evaluators.common.FullTextContainsDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FullTextUtil {
    private static final Logger LOGGER = LogManager.getLogger();

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
    // Otherwise, return the full-text config if one exists in the expression, otherwise return the default config
    public static String getFullTextConfigNameFromExpr(IOptimizableFuncExpr expr) {
        return getFullTextConfigNameFromExpr(expr.getFuncExpr());
    }

    public static String getFullTextConfigNameFromExpr(AbstractFunctionCallExpression funcExpr) {
        if (isFullTextFunctionExpr(funcExpr) == false) {
            return null;
        }

        String configName = FullTextConfig.DEFAULT_FULL_TEXT_CONFIG_NAME;
        List<Mutable<ILogicalExpression>> arguments = funcExpr.getArguments();

        // The first two arguments are
        // 1) the full-text record field to be queried,
        // 2) the query keyword array
        // The next fields are the list of full-text search options,
        // say, the next 4 fields can be "mode", "all", "config", "DEFAULT_FULL_TEXT_CONFIG"
        //
        // Originally, the full-text search option is an Asterix record such as
        //     {"mode": "all", "config": "DEFAULT_FULL_TEXT_CONFIG"},
        // and it is transformed to dedicated fields in the
        // RemoveDuplicateFieldsRule.transform()
        for (int i = 2; i < arguments.size(); i += 2) {
            // The the full-text search option arguments are already checked in FullTextContainsParameterCheckRule,
            String optionName = ConstantExpressionUtil.getStringConstant(arguments.get(i).getValue());

            if (optionName.equalsIgnoreCase(FullTextContainsDescriptor.FULLTEXT_CONFIG_OPTION)) {
                configName = ConstantExpressionUtil.getStringConstant(arguments.get(i + 1).getValue());
                break;
            }
        }

        return configName;
    }

    public static InvertedIndexAccessMethod.SearchModifierType getFullTextSearchModeFromExpr(AbstractFunctionCallExpression funcExpr) {

        // After the third argument, the following arguments are full-text search options.
        for (int i = 2; i < funcExpr.getArguments().size(); i = i + 2) {
            String optionName = ConstantExpressionUtil.getStringArgument(funcExpr, i);

            if (optionName.equals(FullTextContainsDescriptor.SEARCH_MODE_OPTION)) {
                String searchType = ConstantExpressionUtil.getStringArgument(funcExpr, i + 1);

                if (searchType.equals(FullTextContainsDescriptor.CONJUNCTIVE_SEARCH_MODE_OPTION)) {
                    return InvertedIndexAccessMethod.SearchModifierType.CONJUNCTIVE;
                } else {
                    return InvertedIndexAccessMethod.SearchModifierType.DISJUNCTIVE;
                }
            }
        }

        // Use CONJUNCTIVE by default
        return InvertedIndexAccessMethod.SearchModifierType.CONJUNCTIVE;
    }

}