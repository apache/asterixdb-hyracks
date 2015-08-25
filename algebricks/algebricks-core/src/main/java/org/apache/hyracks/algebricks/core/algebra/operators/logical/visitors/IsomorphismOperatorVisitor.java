/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class IsomorphismOperatorVisitor implements ILogicalOperatorVisitor<Boolean, ILogicalOperator> {

    private final Map<LogicalVariable, LogicalVariable> variableMapping = new HashMap<LogicalVariable, LogicalVariable>();

    public IsomorphismOperatorVisitor() {
    }

    @Override
    public Boolean visitAggregateOperator(AggregateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.AGGREGATE)
            return Boolean.FALSE;
        AggregateOperator aggOpArg = (AggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.RUNNINGAGGREGATE)
            return Boolean.FALSE;
        RunningAggregateOperator aggOpArg = (RunningAggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitExtensionOperator(ExtensionOperator op, ILogicalOperator arg) throws AlgebricksException {
        ExtensionOperator aop = (ExtensionOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.EXTENSION_OPERATOR)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        // require the same physical operator, otherwise delivers different data
        // properties
        if (aop.getOperatorTag() != LogicalOperatorTag.GROUP
                || aop.getPhysicalOperator().getOperatorTag() != op.getPhysicalOperator().getOperatorTag())
            return Boolean.FALSE;

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyLists = op.getGroupByList();
        GroupByOperator gbyOpArg = (GroupByOperator) copyAndSubstituteVar(op, arg);
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyListsArg = gbyOpArg.getGroupByList();

        List<Pair<LogicalVariable, ILogicalExpression>> listLeft = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        List<Pair<LogicalVariable, ILogicalExpression>> listRight = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();

        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyLists)
            listLeft.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyListsArg)
            listRight.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));

        boolean isomorphic = VariableUtilities.varListEqualUnordered(listLeft, listRight);

        if (!isomorphic)
            return Boolean.FALSE;
        int sizeOp = op.getNestedPlans().size();
        int sizeArg = gbyOpArg.getNestedPlans().size();
        if (sizeOp != sizeArg)
            return Boolean.FALSE;

        GroupByOperator argOp = (GroupByOperator) arg;
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = argOp.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() != rootsArg.size())
                return Boolean.FALSE;
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                isomorphic = IsomorphismUtilities.isOperatorIsomorphicPlanSegment(topOp1, topOp2);
                if (!isomorphic)
                    return Boolean.FALSE;
            }
        }
        return isomorphic;
    }

    @Override
    public Boolean visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LIMIT)
            return Boolean.FALSE;
        LimitOperator limitOpArg = (LimitOperator) copyAndSubstituteVar(op, arg);
        if (op.getOffset() != limitOpArg.getOffset())
            return Boolean.FALSE;
        boolean isomorphic = op.getMaxObjects().getValue().equals(limitOpArg.getMaxObjects().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INNERJOIN)
            return Boolean.FALSE;
        InnerJoinOperator joinOpArg = (InnerJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN)
            return Boolean.FALSE;
        LeftOuterJoinOperator joinOpArg = (LeftOuterJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ORDER)
            return Boolean.FALSE;
        OrderOperator orderOpArg = (OrderOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareIOrderAndExpressions(op.getOrderExpressions(), orderOpArg.getOrderExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return Boolean.FALSE;
        AssignOperator assignOpArg = (AssignOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(assignOpArg.getVariables(), assignOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SELECT)
            return Boolean.FALSE;
        SelectOperator selectOpArg = (SelectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(selectOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.PROJECT)
            return Boolean.FALSE;
        ProjectOperator projectOpArg = (ProjectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), projectOpArg.getVariables());
        return isomorphic;
    }

    @Override
    public Boolean visitPartitioningSplitOperator(PartitioningSplitOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.PARTITIONINGSPLIT)
            return Boolean.FALSE;
        PartitioningSplitOperator partitionOpArg = (PartitioningSplitOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareExpressions(op.getExpressions(), partitionOpArg.getExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.REPLICATE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.MATERIALIZE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SCRIPT)
            return Boolean.FALSE;
        ScriptOperator scriptOpArg = (ScriptOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getScriptDescription().equals(scriptOpArg.getScriptDescription());
        return isomorphic;
    }

    @Override
    public Boolean visitSplitOperator(SplitOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SPLIT)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SUBPLAN)
            return Boolean.FALSE;
        SubplanOperator subplanOpArg = (SubplanOperator) copyAndSubstituteVar(op, arg);
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = subplanOpArg.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() == rootsArg.size())
                return Boolean.FALSE;
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                boolean isomorphic = IsomorphismUtilities.isOperatorIsomorphicPlanSegment(topOp1, topOp2);
                if (!isomorphic)
                    return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNIONALL)
            return Boolean.FALSE;
        UnionAllOperator unionOpArg = (UnionAllOperator) copyAndSubstituteVar(op, arg);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mapping = op.getVariableMappings();
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mappingArg = unionOpArg.getVariableMappings();
        if (mapping.size() != mappingArg.size())
            return Boolean.FALSE;
        return VariableUtilities.varListEqualUnordered(mapping, mappingArg);
    }

    @Override
    public Boolean visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST)
            return Boolean.FALSE;
        UnnestOperator unnestOpArg = (UnnestOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables())
                && variableEqual(op.getPositionalVariable(), unnestOpArg.getPositionalVariable());
        if (!isomorphic)
            return Boolean.FALSE;
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP)
            return Boolean.FALSE;
        UnnestMapOperator unnestOpArg = (UnnestMapOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables());
        if (!isomorphic)
            return Boolean.FALSE;
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN)
            return Boolean.FALSE;
        DataSourceScanOperator argScan = (DataSourceScanOperator) arg;
        if (!argScan.getDataSource().toString().equals(op.getDataSource().toString()))
            return Boolean.FALSE;
        DataSourceScanOperator scanOpArg = (DataSourceScanOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), scanOpArg.getVariables())
                && op.getDataSource().toString().equals(scanOpArg.getDataSource().toString());
        return isomorphic;
    }

    @Override
    public Boolean visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTINCT)
            return Boolean.FALSE;
        DistinctOperator distinctOpArg = (DistinctOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareExpressions(op.getExpressions(), distinctOpArg.getExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.EXCHANGE)
            return Boolean.FALSE;
        // require the same partition property
        if (!(op.getPhysicalOperator().getOperatorTag() == aop.getPhysicalOperator().getOperatorTag()))
            return Boolean.FALSE;
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, arg, variableMapping);
        IPhysicalPropertiesVector properties = op.getPhysicalOperator().getDeliveredProperties();
        IPhysicalPropertiesVector propertiesArg = aop.getPhysicalOperator().getDeliveredProperties();
        if (properties == null && propertiesArg == null)
            return Boolean.TRUE;
        if (properties == null || propertiesArg == null)
            return Boolean.FALSE;
        IPartitioningProperty partProp = properties.getPartitioningProperty();
        IPartitioningProperty partPropArg = propertiesArg.getPartitioningProperty();
        if (!partProp.getPartitioningType().equals(partPropArg.getPartitioningType()))
            return Boolean.FALSE;
        List<LogicalVariable> columns = new ArrayList<LogicalVariable>();
        partProp.getColumns(columns);
        List<LogicalVariable> columnsArg = new ArrayList<LogicalVariable>();
        partPropArg.getColumns(columnsArg);
        if (columns.size() != columnsArg.size())
            return Boolean.FALSE;
        if (columns.size() == 0)
            return Boolean.TRUE;
        for (int i = 0; i < columnsArg.size(); i++) {
            LogicalVariable rightVar = columnsArg.get(i);
            LogicalVariable leftVar = variableMapping.get(rightVar);
            if (leftVar != null)
                columnsArg.set(i, leftVar);
        }
        return VariableUtilities.varListEqualUnordered(columns, columnsArg);
    }

    @Override
    public Boolean visitWriteOperator(WriteOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE)
            return Boolean.FALSE;
        WriteOperator writeOpArg = (WriteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT)
            return Boolean.FALSE;
        DistributeResultOperator writeOpArg = (DistributeResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT)
            return Boolean.FALSE;
        WriteResultOperator writeOpArg = (WriteResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        if (!op.getDataSource().equals(writeOpArg.getDataSource()))
            isomorphic = false;
        if (!op.getPayloadExpression().equals(writeOpArg.getPayloadExpression()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitInsertDeleteOperator(InsertDeleteOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return Boolean.FALSE;
        InsertDeleteOperator insertOpArg = (InsertDeleteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSource().equals(insertOpArg.getDataSource()))
            isomorphic = false;
        if (!op.getPayloadExpression().equals(insertOpArg.getPayloadExpression()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INDEX_INSERT_DELETE)
            return Boolean.FALSE;
        IndexInsertDeleteOperator insertOpArg = (IndexInsertDeleteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSourceIndex().equals(insertOpArg.getDataSourceIndex()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.TOKENIZE)
            return Boolean.FALSE;
        TokenizeOperator tokenizeOpArg = (TokenizeOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), tokenizeOpArg.getSchema());
        if (!op.getDataSourceIndex().equals(tokenizeOpArg.getDataSourceIndex()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitSinkOperator(SinkOperator op, ILogicalOperator arg) throws AlgebricksException {
        return true;
    }

    private Boolean compareExpressions(List<Mutable<ILogicalExpression>> opExprs,
            List<Mutable<ILogicalExpression>> argExprs) {
        if (opExprs.size() != argExprs.size())
            return Boolean.FALSE;
        for (int i = 0; i < opExprs.size(); i++) {
            boolean isomorphic = opExprs.get(i).getValue().equals(argExprs.get(i).getValue());
            if (!isomorphic)
                return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitExternalDataLookupOperator(ExternalDataLookupOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        return Boolean.FALSE;
    }

    private Boolean compareIOrderAndExpressions(List<Pair<IOrder, Mutable<ILogicalExpression>>> opOrderExprs,
            List<Pair<IOrder, Mutable<ILogicalExpression>>> argOrderExprs) {
        if (opOrderExprs.size() != argOrderExprs.size())
            return Boolean.FALSE;
        for (int i = 0; i < opOrderExprs.size(); i++) {
            boolean isomorphic = opOrderExprs.get(i).first.equals(argOrderExprs.get(i).first);
            if (!isomorphic)
                return Boolean.FALSE;
            isomorphic = opOrderExprs.get(i).second.getValue().equals(argOrderExprs.get(i).second.getValue());
            if (!isomorphic)
                return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private ILogicalOperator copyAndSubstituteVar(ILogicalOperator op, ILogicalOperator argOp)
            throws AlgebricksException {
        ILogicalOperator newOp = OperatorManipulationUtil.deepCopy(argOp);
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, argOp, variableMapping);

        List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
        if (argOp.getInputs().size() > 0)
            for (int i = 0; i < argOp.getInputs().size(); i++)
                VariableUtilities.getLiveVariables(argOp.getInputs().get(i).getValue(), liveVars);
        List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(argOp, producedVars);
        List<LogicalVariable> producedVarsNew = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(op, producedVarsNew);

        if (producedVars.size() != producedVarsNew.size())
            return newOp;
        for (Entry<LogicalVariable, LogicalVariable> map : variableMapping.entrySet()) {
            if (liveVars.contains(map.getKey())) {
                VariableUtilities.substituteVariables(newOp, map.getKey(), map.getValue(), null);
            }
        }
        for (int i = 0; i < producedVars.size(); i++)
            VariableUtilities.substituteVariables(newOp, producedVars.get(i), producedVarsNew.get(i), null);
        return newOp;
    }

    public List<Pair<LogicalVariable, ILogicalExpression>> getPairList(List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> exprs) throws AlgebricksException {
        List<Pair<LogicalVariable, ILogicalExpression>> list = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        if (vars.size() != exprs.size())
            throw new AlgebricksException("variable list size does not equal to expression list size ");
        for (int i = 0; i < vars.size(); i++) {
            list.add(new Pair<LogicalVariable, ILogicalExpression>(vars.get(i), exprs.get(i).getValue()));
        }
        return list;
    }

    private static boolean variableEqual(LogicalVariable var, LogicalVariable varArg) {
        if (var == null && varArg == null)
            return true;
        if (var.equals(varArg))
            return true;
        else
            return false;
    }

}
