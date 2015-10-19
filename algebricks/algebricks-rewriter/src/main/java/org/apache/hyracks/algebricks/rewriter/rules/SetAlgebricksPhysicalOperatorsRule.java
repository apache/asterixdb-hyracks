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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BulkloadPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.DataSourceScanPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.DistributeResultPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.EmptyTupleSourcePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InMemoryStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.IndexBulkloadPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.IndexInsertDeletePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InsertDeletePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.MicroPreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedTupleSourcePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreSortedDistinctByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RunningAggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SinkPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SinkWritePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamLimitPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamSelectPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StringStreamingScriptPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SubplanPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.TokenizePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnionAllPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnnestPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WriteResultPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.rewriter.util.JoinUtils;

public class SetAlgebricksPhysicalOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // if (context.checkIfInDontApplySet(this, op)) {
        // return false;
        // }
        if (op.getPhysicalOperator() != null) {
            return false;
        }

        computeDefaultPhysicalOp(op, true, context);
        // context.addToDontApplySet(this, op);
        return true;
    }

    private static void setPhysicalOperators(ILogicalPlan plan, boolean topLevelOp, IOptimizationContext context)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) root.getValue(), topLevelOp, context);
        }
    }

    @SuppressWarnings("unchecked")
    private static void computeDefaultPhysicalOp(AbstractLogicalOperator op, boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
        if (op.getPhysicalOperator() == null) {
            switch (op.getOperatorTag()) {
                case AGGREGATE: {
                    op.setPhysicalOperator(new AggregatePOperator());
                    break;
                }
                case ASSIGN: {
                    op.setPhysicalOperator(new AssignPOperator());
                    break;
                }
                case DISTINCT: {
                    DistinctOperator distinct = (DistinctOperator) op;
                    distinct.setPhysicalOperator(new PreSortedDistinctByPOperator(distinct.getDistinctByVarList()));
                    break;
                }
                case EMPTYTUPLESOURCE: {
                    op.setPhysicalOperator(new EmptyTupleSourcePOperator());
                    break;
                }
                case EXCHANGE: {
                    if (op.getPhysicalOperator() == null) {
                        throw new AlgebricksException("Implementation for EXCHANGE operator was not set.");
                    }
                    // implem. choice for exchange should be set by a parent op.
                    break;
                }
                case GROUP: {
                    GroupByOperator gby = (GroupByOperator) op;

                    if (gby.getNestedPlans().size() == 1) {
                        ILogicalPlan p0 = gby.getNestedPlans().get(0);
                        if (p0.getRoots().size() == 1) {
                            if (gby.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY) == Boolean.TRUE
                                    || gby.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY) == Boolean.TRUE) {
                                if (!topLevelOp) {
                                    throw new NotImplementedException(
                                            "External hash group-by for nested grouping is not implemented.");
                                }

                                boolean hasIntermediateAgg = generateMergeAggregationExpressions(gby, context);
                                if (hasIntermediateAgg) {
                                    ExternalGroupByPOperator externalGby = new ExternalGroupByPOperator(
                                            gby.getGroupByList(),
                                            physicalOptimizationConfig.getMaxFramesExternalGroupBy(),
                                            physicalOptimizationConfig.getExternalGroupByTableSize());
                                    op.setPhysicalOperator(externalGby);
                                    break;
                                }
                            }
                        }
                    }

                    List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gby.getGroupByList();
                    List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                    for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
                        ILogicalExpression expr = p.second.getValue();
                        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                            columnList.add(varRef.getVariableReference());
                        }
                    }
                    if (topLevelOp) {
                        op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList));
                    } else {
                        op.setPhysicalOperator(new MicroPreclusteredGroupByPOperator(columnList));
                    }
                    break;
                }
                case INNERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((InnerJoinOperator) op, context);
                    break;
                }
                case LEFTOUTERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((LeftOuterJoinOperator) op, context);
                    break;
                }
                case LIMIT: {
                    op.setPhysicalOperator(new StreamLimitPOperator());
                    break;
                }
                case NESTEDTUPLESOURCE: {
                    op.setPhysicalOperator(new NestedTupleSourcePOperator());
                    break;
                }
                case ORDER: {
                    OrderOperator oo = (OrderOperator) op;
                    for (Pair<IOrder, Mutable<ILogicalExpression>> p : oo.getOrderExpressions()) {
                        ILogicalExpression e = p.second.getValue();
                        if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            throw new AlgebricksException("Order expression " + e + " has not been normalized.");
                        }
                    }
                    if (topLevelOp) {
                        op.setPhysicalOperator(new StableSortPOperator(physicalOptimizationConfig
                                .getMaxFramesExternalSort()));
                    } else {
                        op.setPhysicalOperator(new InMemoryStableSortPOperator());
                    }
                    break;
                }
                case PROJECT: {
                    op.setPhysicalOperator(new StreamProjectPOperator());
                    break;
                }
                case RUNNINGAGGREGATE: {
                    op.setPhysicalOperator(new RunningAggregatePOperator());
                    break;
                }
                case REPLICATE: {
                    op.setPhysicalOperator(new ReplicatePOperator());
                    break;
                }
                case SCRIPT: {
                    op.setPhysicalOperator(new StringStreamingScriptPOperator());
                    break;
                }
                case SELECT: {
                    op.setPhysicalOperator(new StreamSelectPOperator());
                    break;
                }
                case SUBPLAN: {
                    op.setPhysicalOperator(new SubplanPOperator());
                    break;
                }
                case UNIONALL: {
                    op.setPhysicalOperator(new UnionAllPOperator());
                    break;
                }

                case UNNEST: {
                    op.setPhysicalOperator(new UnnestPOperator());
                    break;
                }
                case DATASOURCESCAN: {
                    DataSourceScanOperator scan = (DataSourceScanOperator) op;
                    IDataSource dataSource = scan.getDataSource();
                    DataSourceScanPOperator dss = new DataSourceScanPOperator(dataSource);
                    IMetadataProvider mp = context.getMetadataProvider();
                    if (mp.scannerOperatorIsLeaf(dataSource)) {
                        dss.disableJobGenBelowMe();
                    }
                    op.setPhysicalOperator(dss);
                    break;
                }
                case WRITE: {
                    op.setPhysicalOperator(new SinkWritePOperator());
                    break;
                }
                case DISTRIBUTE_RESULT: {
                    op.setPhysicalOperator(new DistributeResultPOperator());
                    break;
                }
                case WRITE_RESULT: {
                    WriteResultOperator opLoad = (WriteResultOperator) op;
                    LogicalVariable payload;
                    List<LogicalVariable> keys = new ArrayList<LogicalVariable>();
                    List<LogicalVariable> additionalFilteringKeys = null;
                    payload = getKeysAndLoad(opLoad.getPayloadExpression(), opLoad.getKeyExpressions(), keys);
                    if (opLoad.getAdditionalFilteringExpressions() != null) {
                        additionalFilteringKeys = new ArrayList<LogicalVariable>();
                        getKeys(opLoad.getAdditionalFilteringExpressions(), additionalFilteringKeys);
                    }
                    op.setPhysicalOperator(new WriteResultPOperator(opLoad.getDataSource(), payload, keys,
                            additionalFilteringKeys));
                    break;
                }
                case INSERT_DELETE: {
                    InsertDeleteOperator opLoad = (InsertDeleteOperator) op;
                    LogicalVariable payload;
                    List<LogicalVariable> keys = new ArrayList<LogicalVariable>();
                    List<LogicalVariable> additionalFilteringKeys = null;
                    payload = getKeysAndLoad(opLoad.getPayloadExpression(), opLoad.getPrimaryKeyExpressions(), keys);
                    if (opLoad.getAdditionalFilteringExpressions() != null) {
                        additionalFilteringKeys = new ArrayList<LogicalVariable>();
                        getKeys(opLoad.getAdditionalFilteringExpressions(), additionalFilteringKeys);
                    }
                    if (opLoad.isBulkload()) {
                        op.setPhysicalOperator(new BulkloadPOperator(payload, keys, additionalFilteringKeys, opLoad
                                .getDataSource()));
                    } else {
                        op.setPhysicalOperator(new InsertDeletePOperator(payload, keys, additionalFilteringKeys, opLoad
                                .getDataSource()));
                    }
                    break;
                }
                case INDEX_INSERT_DELETE: {
                    IndexInsertDeleteOperator opInsDel = (IndexInsertDeleteOperator) op;
                    List<LogicalVariable> primaryKeys = new ArrayList<LogicalVariable>();
                    List<LogicalVariable> secondaryKeys = new ArrayList<LogicalVariable>();
                    List<LogicalVariable> additionalFilteringKeys = null;
                    getKeys(opInsDel.getPrimaryKeyExpressions(), primaryKeys);
                    getKeys(opInsDel.getSecondaryKeyExpressions(), secondaryKeys);
                    if (opInsDel.getAdditionalFilteringExpressions() != null) {
                        additionalFilteringKeys = new ArrayList<LogicalVariable>();
                        getKeys(opInsDel.getAdditionalFilteringExpressions(), additionalFilteringKeys);
                    }
                    if (opInsDel.isBulkload()) {
                        op.setPhysicalOperator(new IndexBulkloadPOperator(primaryKeys, secondaryKeys,
                                additionalFilteringKeys, opInsDel.getFilterExpression(), opInsDel.getDataSourceIndex()));
                    } else {
                        op.setPhysicalOperator(new IndexInsertDeletePOperator(primaryKeys, secondaryKeys,
                                additionalFilteringKeys, opInsDel.getFilterExpression(), opInsDel.getDataSourceIndex()));
                    }

                    break;

                }
                case TOKENIZE: {
                    TokenizeOperator opTokenize = (TokenizeOperator) op;
                    List<LogicalVariable> primaryKeys = new ArrayList<LogicalVariable>();
                    List<LogicalVariable> secondaryKeys = new ArrayList<LogicalVariable>();
                    getKeys(opTokenize.getPrimaryKeyExpressions(), primaryKeys);
                    getKeys(opTokenize.getSecondaryKeyExpressions(), secondaryKeys);
                    // Tokenize Operator only operates with a bulk load on a data set with an index
                    if (opTokenize.isBulkload()) {
                        op.setPhysicalOperator(new TokenizePOperator(primaryKeys, secondaryKeys, opTokenize
                                .getDataSourceIndex()));
                    }
                    break;
                }
                case SINK: {
                    op.setPhysicalOperator(new SinkPOperator());
                    break;
                }
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                setPhysicalOperators(p, false, context);
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) opRef.getValue(), topLevelOp, context);
        }
    }

    private static void getKeys(List<Mutable<ILogicalExpression>> keyExpressions, List<LogicalVariable> keys) {
        for (Mutable<ILogicalExpression> kExpr : keyExpressions) {
            ILogicalExpression e = kExpr.getValue();
            if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new NotImplementedException();
            }
            keys.add(((VariableReferenceExpression) e).getVariableReference());
        }
    }

    private static LogicalVariable getKeysAndLoad(Mutable<ILogicalExpression> payloadExpr,
            List<Mutable<ILogicalExpression>> keyExpressions, List<LogicalVariable> keys) {
        LogicalVariable payload;
        if (payloadExpr.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            throw new NotImplementedException();
        }
        payload = ((VariableReferenceExpression) payloadExpr.getValue()).getVariableReference();

        for (Mutable<ILogicalExpression> kExpr : keyExpressions) {
            ILogicalExpression e = kExpr.getValue();
            if (e.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new NotImplementedException();
            }
            keys.add(((VariableReferenceExpression) e).getVariableReference());
        }
        return payload;
    }

    private static boolean generateMergeAggregationExpressions(GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        if (gby.getNestedPlans().size() != 1) {
            //External/Sort group-by currently works only for one nested plan with one root containing
            //an aggregate and a nested-tuple-source.
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            //External/Sort group-by currently works only for one nested plan with one root containing
            //an aggregate and a nested-tuple-source.
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        IMergeAggregationExpressionFactory mergeAggregationExpressionFactory = context
                .getMergeAggregationExpressionFactory();
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AbstractLogicalOperator r0Logical = (AbstractLogicalOperator) r0.getValue();
        if (r0Logical.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
        List<LogicalVariable> originalAggVars = aggOp.getVariables();
        int n = aggOp.getExpressions().size();
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory.createMergeAggregation(
                    originalAggVars.get(i), aggFuncRefs.get(i).getValue(), context);
            if (mergeExpr == null) {
                return false;
            }
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
        return true;
    }
}
