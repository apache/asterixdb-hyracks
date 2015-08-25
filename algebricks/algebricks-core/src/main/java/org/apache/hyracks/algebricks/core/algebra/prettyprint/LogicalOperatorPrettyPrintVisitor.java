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
package edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator.Kind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LogicalOperatorPrettyPrintVisitor implements ILogicalOperatorVisitor<String, Integer> {

    ILogicalExpressionVisitor<String, Integer> exprVisitor;

    public LogicalOperatorPrettyPrintVisitor() {
        exprVisitor = new LogicalExpressionPrettyPrintVisitor();
    }

    public LogicalOperatorPrettyPrintVisitor(ILogicalExpressionVisitor<String, Integer> exprVisitor) {
        this.exprVisitor = exprVisitor;
    }

    @Override
    public String visitAggregateOperator(AggregateOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("aggregate ").append(op.getVariables()).append(" <- ");
        pprintExprList(op.getExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitRunningAggregateOperator(RunningAggregateOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("running-aggregate ").append(op.getVariables()).append(" <- ");
        pprintExprList(op.getExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("empty-tuple-source");
        return buffer.toString();
    }

    @Override
    public String visitGroupByOperator(GroupByOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("group by (");
        pprintVeList(buffer, op.getGroupByList(), indent);
        buffer.append(") decor (");
        pprintVeList(buffer, op.getDecorList(), indent);
        buffer.append(") {");
        printNestedPlans(op, indent, buffer);
        return buffer.toString();
    }

    @Override
    public String visitDistinctOperator(DistinctOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("distinct " + "(");
        pprintExprList(op.getExpressions(), buffer, indent);
        buffer.append(")");
        return buffer.toString();
    }

    @Override
    public String visitInnerJoinOperator(InnerJoinOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("join (").append(op.getCondition().getValue().accept(exprVisitor, indent))
                .append(")");
        return buffer.toString();
    }

    @Override
    public String visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("left outer join (")
                .append(op.getCondition().getValue().accept(exprVisitor, indent)).append(")");
        return buffer.toString();
    }

    @Override
    public String visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("nested tuple source");
        return buffer.toString();
    }

    @Override
    public String visitOrderOperator(OrderOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("order ");
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            String fst;
            switch (p.first.getKind()) {
                case ASC: {
                    fst = "ASC";
                    break;
                }
                case DESC: {
                    fst = "DESC";
                    break;
                }
                default: {
                    fst = p.first.getExpressionRef().toString();
                }
            }
            buffer.append("(" + fst + ", " + p.second.getValue().accept(exprVisitor, indent) + ") ");
        }
        return buffer.toString();
    }

    @Override
    public String visitAssignOperator(AssignOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("assign ").append(op.getVariables()).append(" <- ");
        pprintExprList(op.getExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitWriteOperator(WriteOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("write ");
        pprintExprList(op.getExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitDistributeResultOperator(DistributeResultOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("distribute result ");
        pprintExprList(op.getExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitWriteResultOperator(WriteResultOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("load ").append(op.getDataSource()).append(" from ")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent)).append(" partitioned by ");
        pprintExprList(op.getKeyExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitSelectOperator(SelectOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("select (").append(op.getCondition().getValue().accept(exprVisitor, indent))
                .append(")");
        return buffer.toString();
    }

    @Override
    public String visitProjectOperator(ProjectOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("project " + "(" + op.getVariables() + ")");
        return buffer.toString();
    }

    @Override
    public String visitPartitioningSplitOperator(PartitioningSplitOperator op, Integer indent)
            throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("partitioning-split (");
        pprintExprList(op.getExpressions(), buffer, indent);
        buffer.append(")");
        return buffer.toString();
    }

    @Override
    public String visitSubplanOperator(SubplanOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("subplan {");
        printNestedPlans(op, indent, buffer);
        return buffer.toString();
    }

    @Override
    public String visitUnionOperator(UnionAllOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("union");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            buffer.append(" (" + v.first + ", " + v.second + ", " + v.third + ")");
        }
        return buffer.toString();
    }

    @Override
    public String visitUnnestOperator(UnnestOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("unnest " + op.getVariable());
        if (op.getPositionalVariable() != null) {
            buffer.append(" at " + op.getPositionalVariable());
        }
        buffer.append(" <- " + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        return buffer.toString();
    }

    @Override
    public String visitUnnestMapOperator(UnnestMapOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append(
                "unnest-map " + op.getVariables() + " <- "
                        + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        return buffer.toString();
    }

    @Override
    public String visitDataScanOperator(DataSourceScanOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append(
                "data-scan " + op.getProjectVariables() + "<-" + op.getVariables() + " <- " + op.getDataSource());
        return buffer.toString();
    }

    @Override
    public String visitLimitOperator(LimitOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("limit " + op.getMaxObjects().getValue().accept(exprVisitor, indent));
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            buffer.append(", " + offset.accept(exprVisitor, indent));
        }
        return buffer.toString();
    }

    @Override
    public String visitExchangeOperator(ExchangeOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("exchange ");
        return buffer.toString();
    }

    @Override
    public String visitReplicateOperator(ReplicateOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("replicate ");
        return buffer.toString();
    }

    @Override
    public String visitScriptOperator(ScriptOperator op, Integer indent) {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append(
                "script (in: " + op.getInputVariables() + ") (out: " + op.getOutputVariables() + ")");
        return buffer.toString();
    }

    @Override
    public String visitSplitOperator(SplitOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        LogicalVariable conditionalSplitVar = op.getConditionVar();
        addIndent(buffer, indent).append("split [" + conditionalSplitVar + "]");
        return buffer.toString();
    }

    @Override
    public String visitMaterializeOperator(MaterializeOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("materialize ");
        return buffer.toString();
    }

    @Override
    public String visitInsertDeleteOperator(InsertDeleteOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        String header = op.getOperation() == Kind.INSERT ? "insert into " : "delete from ";
        addIndent(buffer, indent).append(header).append(op.getDataSource()).append(" from ")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent)).append(" partitioned by ");
        pprintExprList(op.getPrimaryKeyExpressions(), buffer, indent);
        if (op.isBulkload()) {
            buffer.append(" [bulkload]");
        }
        return buffer.toString();
    }

    @Override
    public String visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, Integer indent)
            throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        String header = op.getOperation() == Kind.INSERT ? "insert into " : "delete from ";
        addIndent(buffer, indent).append(header).append(op.getIndexName()).append(" on ")
                .append(op.getDataSourceIndex().getDataSource()).append(" from ");
        pprintExprList(op.getSecondaryKeyExpressions(), buffer, indent);
        if (op.isBulkload()) {
            buffer.append(" [bulkload]");
        }
        return buffer.toString();
    }

    @Override
    public String visitTokenizeOperator(TokenizeOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("tokenize ").append(op.getTokenizeVars()).append(" <- ");
        pprintExprList(op.getSecondaryKeyExpressions(), buffer, indent);
        return buffer.toString();
    }

    @Override
    public String visitSinkOperator(SinkOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append("sink");
        return buffer.toString();
    }

    @Override
    public String visitExtensionOperator(ExtensionOperator op, Integer indent) throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append(op.toString());
        return buffer.toString();
    }

    protected static final StringBuilder addIndent(StringBuilder buffer, int level) {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
    }

    protected void printNestedPlans(AbstractOperatorWithNestedPlans op, Integer indent, StringBuilder buffer)
            throws AlgebricksException {
        boolean first = true;
        if (op.getNestedPlans().isEmpty()) {
            buffer.append("}");
        } else {
            for (ILogicalPlan p : op.getNestedPlans()) {
                // PrettyPrintUtil.indent(buffer, level + 10).append("var " +
                // p.first + ":\n");
                buffer.append("\n");
                if (first) {
                    first = false;
                } else {
                    addIndent(buffer, indent).append("       {\n");
                }
                PlanPrettyPrinter.printPlan(p, buffer, this, indent + 10);
                addIndent(buffer, indent).append("       }");
            }
        }
    }

    protected void pprintExprList(List<Mutable<ILogicalExpression>> expressions, StringBuilder buffer, Integer indent)
            throws AlgebricksException {
        buffer.append("[");
        boolean first = true;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(exprRef.getValue().accept(exprVisitor, indent));
        }
        buffer.append("]");
    }

    protected void pprintVeList(StringBuilder sb, List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList,
            Integer indent) throws AlgebricksException {
        sb.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                sb.append("; ");
            }
            if (ve.first != null) {
                sb.append(ve.first + " := " + ve.second);
            } else {
                sb.append(ve.second.getValue().accept(exprVisitor, indent));
            }
        }
        sb.append("]");
    }

    @Override
    public String visitExternalDataLookupOperator(ExternalDataLookupOperator op, Integer indent)
            throws AlgebricksException {
        StringBuilder buffer = new StringBuilder();
        addIndent(buffer, indent).append(
                "external-instant-lookup " + op.getVariables() + " <- " + op.getExpressionRef().getValue());
        return buffer.toString();
    }

}
