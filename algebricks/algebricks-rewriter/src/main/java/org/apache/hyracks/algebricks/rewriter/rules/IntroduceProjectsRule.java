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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Projects away unused variables at the earliest possible point.
 * Does a full DFS sweep of the plan adding ProjectOperators in the bottom-up pass.
 * Also, removes projects that have become useless.
 * TODO: This rule 'recklessly' adds as many projects as possible, but there is no guarantee
 * that the overall cost of the plan is reduced since project operators also add a cost.
 */
public class IntroduceProjectsRule implements IAlgebraicRewriteRule {

    private final Set<LogicalVariable> usedVars = new HashSet<LogicalVariable>();
    private final Set<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
    private final Set<LogicalVariable> producedVars = new HashSet<LogicalVariable>();
    private final List<LogicalVariable> projectVars = new ArrayList<LogicalVariable>();
    protected boolean hasRun = false;
    // Keep track of used variables after the current operator, including used variables in itself.
    private final HashMap<AbstractLogicalOperator, HashSet<LogicalVariable>> allUsedVarsAfterOpMap = new HashMap<AbstractLogicalOperator, HashSet<LogicalVariable>>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (hasRun) {
            return false;
        }
        hasRun = true;

        // Collect all used variables after each operator, including the used variables in itself in the plan.
        Set<LogicalVariable> parentUsedVars = new HashSet<LogicalVariable>();
        collectUsedVars(opRef, parentUsedVars);

        // Introduce projects
        return introduceProjects(null, -1, opRef, Collections.<LogicalVariable> emptySet(), context);
    }

    // Collect all used variables after each operator, including the used variables in itself in the plan.
    // Collecting information is required since there can be multiple paths in the plan and introduceProjects()
    // can deal with only one path at a time during conducting depth-first-search.
    protected void collectUsedVars(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> parentUsedVars)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        HashSet<LogicalVariable> usedVarsPerOp = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, usedVarsPerOp);
        usedVarsPerOp.addAll(parentUsedVars);

        if (allUsedVarsAfterOpMap.get(op) == null) {
            allUsedVarsAfterOpMap.put(op, usedVarsPerOp);
        } else {
            allUsedVarsAfterOpMap.get(op).addAll(usedVarsPerOp);
        }

        for (int i = 0; i < op.getInputs().size(); i++) {
            Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(i);
            collectUsedVars(inputOpRef, usedVarsPerOp);
        }

    }

    protected boolean introduceProjects(AbstractLogicalOperator parentOp, int parentInputIndex,
            Mutable<ILogicalOperator> opRef, Set<LogicalVariable> parentUsedVars, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean modified = false;

        usedVars.clear();
        VariableUtilities.getUsedVariables(op, usedVars);

        // In the top-down pass, maintain a set of variables that are used in op and all its parents.
        // This is a necessary step for the newly created project operator during this optimization,
        // since we already have all information from collectUsedVars() method for the other operators.
        HashSet<LogicalVariable> parentsUsedVars = new HashSet<LogicalVariable>();
        parentsUsedVars.addAll(parentUsedVars);
        parentsUsedVars.addAll(usedVars);

        if (allUsedVarsAfterOpMap.get(op) != null) {
            parentsUsedVars.addAll(allUsedVarsAfterOpMap.get(op));
        }

        // Descend into children.
        for (int i = 0; i < op.getInputs().size(); i++) {
            Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(i);
            if (introduceProjects(op, i, inputOpRef, parentsUsedVars, context)) {
                modified = true;
            }
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        // In the bottom-up pass, determine which live variables are not used by op's parents.
        // Such variables are be projected away.
        liveVars.clear();
        VariableUtilities.getLiveVariables(op, liveVars);
        producedVars.clear();
        VariableUtilities.getProducedVariables(op, producedVars);
        liveVars.removeAll(producedVars);

        projectVars.clear();
        for (LogicalVariable liveVar : liveVars) {
            if (parentsUsedVars.contains(liveVar)) {
                projectVars.add(liveVar);
            }
        }

        // Some of the variables that are live at this op are not used above.
        if (projectVars.size() != liveVars.size()) {
            // Add a project operator under each of op's qualifying input branches.
            for (int i = 0; i < op.getInputs().size(); i++) {
                ILogicalOperator childOp = op.getInputs().get(i).getValue();
                liveVars.clear();
                VariableUtilities.getLiveVariables(childOp, liveVars);
                List<LogicalVariable> vars = new ArrayList<LogicalVariable>();
                vars.addAll(projectVars);
                // Only retain those variables that are live in the i-th input branch.
                vars.retainAll(liveVars);
                // Not push down Project below SPLIT or REPLICATE since it has two or more parents node
                // and this rule checks only one path at a time.
                boolean needToSkip = op.getOperatorTag() == LogicalOperatorTag.SPLIT
                        || op.getOperatorTag() == LogicalOperatorTag.REPLICATE;
                if (vars.size() != liveVars.size() && !needToSkip) {
                    ProjectOperator projectOp = new ProjectOperator(vars);
                    projectOp.getInputs().add(new MutableObject<ILogicalOperator>(childOp));
                    op.getInputs().get(i).setValue(projectOp);
                    context.computeAndSetTypeEnvironmentForOperator(projectOp);
                    modified = true;
                }
            }
        } else if (op.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            // Check if the existing project has become useless.
            liveVars.clear();
            VariableUtilities.getLiveVariables(op.getInputs().get(0).getValue(), liveVars);
            ProjectOperator projectOp = (ProjectOperator) op;
            List<LogicalVariable> projectVars = projectOp.getVariables();
            if (liveVars.size() == projectVars.size() && liveVars.containsAll(projectVars)) {
                boolean eliminateProject = true;
                // For UnionAll the variables must also be in exactly the correct order.
                if (parentOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                    eliminateProject = canEliminateProjectBelowUnion((UnionAllOperator) parentOp, projectOp,
                            parentInputIndex);
                }
                if (eliminateProject) {
                    // The existing project has become useless. Remove it.
                    parentOp.getInputs().get(parentInputIndex).setValue(op.getInputs().get(0).getValue());
                }
            }
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        return modified;
    }

    private boolean canEliminateProjectBelowUnion(UnionAllOperator unionOp, ProjectOperator projectOp,
            int unionInputIndex) throws AlgebricksException {
        List<LogicalVariable> orderedLiveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(projectOp.getInputs().get(0).getValue(), orderedLiveVars);
        int numVars = orderedLiveVars.size();
        for (int i = 0; i < numVars; i++) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varTriple = unionOp.getVariableMappings().get(i);
            if (unionInputIndex == 0) {
                if (varTriple.first != orderedLiveVars.get(i)) {
                    return false;
                }
            } else {
                if (varTriple.second != orderedLiveVars.get(i)) {
                    return false;
                }
            }
        }
        return true;
    }
}
