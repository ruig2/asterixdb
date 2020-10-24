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
package org.apache.asterix.jobgen;

import java.util.List;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.library.ExternalFunctionDescriptorProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.optimizer.rules.util.FullTextUtil;
import org.apache.asterix.runtime.evaluators.common.FullTextContainsDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigDescriptor;

public class QueryLogicalExpressionJobGen implements ILogicalExpressionJobGen {

    private final IFunctionManager functionManager;

    public QueryLogicalExpressionJobGen(IFunctionManager functionManager) {
        this.functionManager = functionManager;
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        IScalarEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd = resolveFunction(expr, env, context);
        switch (fd.getFunctionDescriptorTag()) {
            case SERIALAGGREGATE:
                return null;
            case AGGREGATE:
                return fd.createAggregateEvaluatorFactory(args);
            default:
                throw new IllegalStateException(
                        "Invalid function descriptor " + fd.getFunctionDescriptorTag() + " expected "
                                + FunctionDescriptorTag.SERIALAGGREGATE + " or " + FunctionDescriptorTag.AGGREGATE);
        }
    }

    @Override
    public IRunningAggregateEvaluatorFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        IScalarEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        return resolveFunction(expr, env, context).createRunningAggregateEvaluatorFactory(args);
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        IScalarEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        return resolveFunction(expr, env, context).createUnnestingEvaluatorFactory(args);
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        IScalarEvaluatorFactory copyEvaluatorFactory;
        switch (expr.getExpressionTag()) {
            case VARIABLE: {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                copyEvaluatorFactory = createVariableEvaluatorFactory(v, inputSchemas);
                return copyEvaluatorFactory;
            }
            case CONSTANT: {
                ConstantExpression c = (ConstantExpression) expr;
                copyEvaluatorFactory = createConstantEvaluatorFactory(c, context);
                return copyEvaluatorFactory;
            }
            case FUNCTION_CALL: {
                copyEvaluatorFactory = createScalarFunctionEvaluatorFactory((AbstractFunctionCallExpression) expr, env,
                        inputSchemas, context);
                return copyEvaluatorFactory;
            }
            default:
                throw new IllegalStateException();
        }

    }

    private IScalarEvaluatorFactory createVariableEvaluatorFactory(VariableReferenceExpression expr,
            IOperatorSchema[] inputSchemas) throws AlgebricksException {
        LogicalVariable variable = expr.getVariableReference();
        for (IOperatorSchema scm : inputSchemas) {
            int pos = scm.findVariable(variable);
            if (pos >= 0) {
                return new ColumnAccessEvalFactory(pos);
            }
        }
        throw new AlgebricksException("Variable " + variable + " could not be found in any input schema.");
    }

    private IScalarEvaluatorFactory createScalarFunctionEvaluatorFactory(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        IScalarEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd = null;
        if (expr.getFunctionInfo() instanceof IExternalFunctionInfo) {
            // Expr is an external function
            fd = ExternalFunctionDescriptorProvider
                    .getExternalFunctionDescriptor((IExternalFunctionInfo) expr.getFunctionInfo());
            CompilerProperties props = ((IApplicationContext) context.getAppContext()).getCompilerProperties();
            FunctionTypeInferers.SET_ARGUMENTS_TYPE.infer(expr, fd, env, props);
        } else if (FullTextUtil.isFullTextFunctionExpr(expr)) {
            // Expr is a special internal (built-in) function: ftcontains()
            // it is different from a general built-in function because it needs a parameter from the metadataProvider
            // The parameter is the full-text configuration which will be used to tokenize and process tokens,
            // e.g. via a stopwords full-text filter to discard stopwords
            // If the user didn't specify a full-text config name, then a default one will be used
            //
            // Currently, this is the only function that needs a parameter from metadataProvider,
            // so let's treat it differently and exclude it from the following resolveFunction() case
            // In the future, if we have more functions that need to be parameterized,
            // then maybe we can create a more general interface for those parameterize-able functions.
            String fullTextConfigName = FullTextUtil.getFullTextConfigNameFromExpr(expr);
            // ToDo: is namespace the data verse?
            String namespace = FullTextUtil.getFullTextConfigDataverseNameFromExpr(expr);
            IFullTextConfigDescriptor configDescriptor = ((MetadataProvider) context.getMetadataProvider())
                    .findFullTextConfigDescriptor(DataverseName.createFromCanonicalForm(namespace), fullTextConfigName);
            if (configDescriptor == null) {
                throw new AsterixException(ErrorCode.FULL_TEXT_CONFIG_NOT_FOUND, fullTextConfigName);
            }
            fd = FullTextContainsDescriptor.createFunctionDescriptor(configDescriptor);
            fd.setSourceLocation(expr.getSourceLocation());
        } else {
            // Expr is an internal (built-in) function
            fd = resolveFunction(expr, env, context);
        }
        return fd.createEvaluatorFactory(args);
    }

    private IScalarEvaluatorFactory createConstantEvaluatorFactory(ConstantExpression expr, JobGenContext context)
            throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        return metadataProvider.getDataFormat().getConstantEvalFactory(expr.getValue());
    }

    private IScalarEvaluatorFactory[] codegenArguments(AbstractFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> arguments = expr.getArguments();
        int n = arguments.size();
        IScalarEvaluatorFactory[] args = new IScalarEvaluatorFactory[n];
        int i = 0;
        for (Mutable<ILogicalExpression> a : arguments) {
            args[i++] = createEvaluatorFactory(a.getValue(), env, inputSchemas, context);
        }
        return args;
    }

    @Override
    public ISerializedAggregateEvaluatorFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        IScalarEvaluatorFactory[] args = codegenArguments(expr, env, inputSchemas, context);
        IFunctionDescriptor fd = resolveFunction(expr, env, context);

        switch (fd.getFunctionDescriptorTag()) {
            case AGGREGATE: {
                if (BuiltinFunctions.isAggregateFunctionSerializable(fd.getIdentifier())) {
                    AggregateFunctionCallExpression serialAggExpr = BuiltinFunctions
                            .makeSerializableAggregateFunctionExpression(fd.getIdentifier(), expr.getArguments());
                    IFunctionDescriptor afdd = resolveFunction(serialAggExpr, env, context);
                    return afdd.createSerializableAggregateEvaluatorFactory(args);
                } else {
                    throw new AlgebricksException(
                            "Trying to create a serializable aggregate from a non-serializable aggregate function descriptor. (fi="
                                    + expr.getFunctionIdentifier() + ")");
                }
            }
            case SERIALAGGREGATE: {
                return fd.createSerializableAggregateEvaluatorFactory(args);
            }

            default:
                throw new IllegalStateException(
                        "Invalid function descriptor " + fd.getFunctionDescriptorTag() + " expected "
                                + FunctionDescriptorTag.SERIALAGGREGATE + " or " + FunctionDescriptorTag.AGGREGATE);
        }
    }

    private IFunctionDescriptor resolveFunction(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            JobGenContext context) throws AlgebricksException {
        FunctionIdentifier fnId = expr.getFunctionIdentifier();
        SourceLocation sourceLocation = expr.getSourceLocation();
        IFunctionDescriptor fd = functionManager.lookupFunction(fnId, sourceLocation);
        fd.setSourceLocation(sourceLocation);
        IFunctionTypeInferer fnTypeInfer = functionManager.lookupFunctionTypeInferer(fnId);
        if (fnTypeInfer != null) {
            CompilerProperties compilerProps = ((IApplicationContext) context.getAppContext()).getCompilerProperties();
            fnTypeInfer.infer(expr, fd, env, compilerProps);
        }
        return fd;
    }
}
