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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes unused variables from Assign, Unnest, Aggregate, and UnionAll operators.
 */
public class RemoveUnusedAssignAndAggregateRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        // Keep the variables that are produced by ASSIGN, UNNEST, AGGREGATE, and UNION operators.
        Set<LogicalVariable> assignedVarSet = new HashSet<LogicalVariable>();
        // Keep the variables that are used after ASSIGN, UNNEST, AGGREGATE, and UNION operators.
        Set<LogicalVariable> usedVarSet = new HashSet<LogicalVariable>();
        collectUnusedAssignedVars((AbstractLogicalOperator) opRef.getValue(), assignedVarSet, usedVarSet, true, context);
        // Remove the variables used after ASSIGN, UNNEST, AGGREGATE, and UNION operators in the assignedVarSet
        // before deleting the ASSIGN expressions.
        Iterator<LogicalVariable> iterator = assignedVarSet.iterator();
        while (iterator.hasNext()) {
            LogicalVariable v = iterator.next();
            if (usedVarSet.contains(v)) {
                iterator.remove();
            }
        }
        boolean smthToRemove = !assignedVarSet.isEmpty();
        if (smthToRemove) {
            removeUnusedAssigns(opRef, assignedVarSet, context);
        }
        return !assignedVarSet.isEmpty();
    }

    private void removeUnusedAssigns(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> toRemove,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        while (removeFromAssigns(op, toRemove, context) == 0) {
            if (op.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                break;
            }
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            opRef.setValue(op);
        }
        Iterator<Mutable<ILogicalOperator>> childIter = op.getInputs().iterator();
        while (childIter.hasNext()) {
            Mutable<ILogicalOperator> cRef = childIter.next();
            removeUnusedAssigns(cRef, toRemove, context);
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNest = (AbstractOperatorWithNestedPlans) op;
            Iterator<ILogicalPlan> planIter = opWithNest.getNestedPlans().iterator();
            while (planIter.hasNext()) {
                ILogicalPlan p = planIter.next();
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    removeUnusedAssigns(r, toRemove, context);
                }
            }

            // Removes redundant nested plans that produces nothing
            for (int i = opWithNest.getNestedPlans().size() - 1; i >= 0; i--) {
                ILogicalPlan nestedPlan = opWithNest.getNestedPlans().get(i);
                List<Mutable<ILogicalOperator>> rootsToBeRemoved = new ArrayList<Mutable<ILogicalOperator>>();
                for (Mutable<ILogicalOperator> r : nestedPlan.getRoots()) {
                    ILogicalOperator topOp = r.getValue();
                    Set<LogicalVariable> producedVars = new ListSet<LogicalVariable>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(topOp, producedVars);
                    if (producedVars.size() == 0) {
                        rootsToBeRemoved.add(r);
                    }
                }
                // Makes sure the operator should have at least ONE nested plan even it is empty
                // (because a lot of places uses this assumption,  TODO(yingyib): clean them up).
                if (nestedPlan.getRoots().size() == rootsToBeRemoved.size() && opWithNest.getNestedPlans().size() > 1) {
                    nestedPlan.getRoots().removeAll(rootsToBeRemoved);
                    opWithNest.getNestedPlans().remove(nestedPlan);
                }
            }
        }
    }

    private int removeFromAssigns(AbstractLogicalOperator op, Set<LogicalVariable> toRemove,
            IOptimizationContext context) throws AlgebricksException {
        switch (op.getOperatorTag()) {
            case ASSIGN: {
                AssignOperator assign = (AssignOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, assign.getVariables(), assign.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(assign);
                }
                return assign.getVariables().size();
            }
            case AGGREGATE: {
                AggregateOperator agg = (AggregateOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, agg.getVariables(), agg.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(agg);
                }
                return agg.getVariables().size();
            }
            case UNNEST: {
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null && toRemove.contains(pVar)) {
                    uOp.setPositionalVariable(null);
                }
                break;
            }
            case UNIONALL: {
                UnionAllOperator unionOp = (UnionAllOperator) op;
                if (removeUnusedVarsFromUnionAll(unionOp, toRemove)) {
                    context.computeAndSetTypeEnvironmentForOperator(unionOp);
                }
                return unionOp.getVariableMappings().size();
            }
        }
        return -1;
    }

    private boolean removeUnusedVarsFromUnionAll(UnionAllOperator unionOp, Set<LogicalVariable> toRemove) {
        Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> iter = unionOp.getVariableMappings()
                .iterator();
        boolean modified = false;
        Set<LogicalVariable> removeFromRemoveSet = new HashSet<LogicalVariable>();
        while (iter.hasNext()) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = iter.next();
            if (toRemove.contains(varMapping.third)) {
                iter.remove();
                modified = true;
            }
            // In any case, make sure we do not removing these variables.
            removeFromRemoveSet.add(varMapping.first);
            removeFromRemoveSet.add(varMapping.second);
        }
        toRemove.removeAll(removeFromRemoveSet);
        return modified;
    }

    private boolean removeUnusedVarsAndExprs(Set<LogicalVariable> toRemove, List<LogicalVariable> varList,
            List<Mutable<ILogicalExpression>> exprList) {
        boolean changed = false;
        Iterator<LogicalVariable> varIter = varList.iterator();
        Iterator<Mutable<ILogicalExpression>> exprIter = exprList.iterator();
        while (varIter.hasNext()) {
            LogicalVariable v = varIter.next();
            exprIter.next();
            if (toRemove.contains(v)) {
                varIter.remove();
                exprIter.remove();
                changed = true;
            }
        }
        return changed;
    }

    private void collectUnusedAssignedVars(AbstractLogicalOperator op, Set<LogicalVariable> assignedVarSet,
            Set<LogicalVariable> usedVarSet, boolean first, IOptimizationContext context) throws AlgebricksException {
        if (!first) {
            context.addToDontApplySet(this, op);
        }
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            collectUnusedAssignedVars((AbstractLogicalOperator) c.getValue(), assignedVarSet, usedVarSet, false,
                    context);
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan plan : opWithNested.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                    collectUnusedAssignedVars((AbstractLogicalOperator) r.getValue(), assignedVarSet, usedVarSet,
                            false, context);
                }
            }
        }
        boolean removeUsedVars = true;
        switch (op.getOperatorTag()) {
            case ASSIGN: {
                AssignOperator assign = (AssignOperator) op;
                assignedVarSet.addAll(assign.getVariables());
                break;
            }
            case AGGREGATE: {
                AggregateOperator agg = (AggregateOperator) op;
                assignedVarSet.addAll(agg.getVariables());
                break;
            }
            case UNNEST: {
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null) {
                    assignedVarSet.add(pVar);
                }
                break;
            }
            case UNIONALL: {
                UnionAllOperator unionOp = (UnionAllOperator) op;
                for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping : unionOp
                        .getVariableMappings()) {
                    assignedVarSet.add(varMapping.third);
                }
                removeUsedVars = false;
                break;
            }
        }

        if (removeUsedVars) {
            List<LogicalVariable> used = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(op, used);
            usedVarSet.addAll(used);
            //            assignedVarSet.removeAll(used);
        }
    }

}
