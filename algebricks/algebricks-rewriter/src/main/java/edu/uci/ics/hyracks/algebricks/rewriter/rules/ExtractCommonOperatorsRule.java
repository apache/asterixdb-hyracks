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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ExtractCommonOperatorsRule implements IAlgebraicRewriteRule {

    private final HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> childrenToParents = new HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>>();
    private final List<Mutable<ILogicalOperator>> roots = new ArrayList<Mutable<ILogicalOperator>>();
    private final List<List<Mutable<ILogicalOperator>>> equivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();
    private final HashMap<Mutable<ILogicalOperator>, BitSet> opToCandidateInputs = new HashMap<Mutable<ILogicalOperator>, BitSet>();
    private final HashMap<Mutable<ILogicalOperator>, MutableInt> clusterMap = new HashMap<Mutable<ILogicalOperator>, MutableInt>();
    private final HashMap<Integer, BitSet> clusterWaitForMap = new HashMap<Integer, BitSet>();
    private int lastUsedClusterId = 0;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        if (!roots.contains(op))
            roots.add(new MutableObject<ILogicalOperator>(op));
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        boolean rewritten = false;
        boolean changed = false;
        if (roots.size() > 0) {
            do {
                changed = false;
                // applying the rewriting until fixpoint
                topDownMaterialization(roots);
                genCandidates(context);
                removeTrivialShare();
                if (equivalenceClasses.size() > 0)
                    changed = rewrite(context);
                if (!rewritten)
                    rewritten = changed;
                equivalenceClasses.clear();
                childrenToParents.clear();
                opToCandidateInputs.clear();
                clusterMap.clear();
                clusterWaitForMap.clear();
                lastUsedClusterId = 0;
            } while (changed);
            roots.clear();
        }
        return rewritten;
    }

    private void removeTrivialShare() {
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            for (int i = candidates.size() - 1; i >= 0; i--) {
                Mutable<ILogicalOperator> opRef = candidates.get(i);
                AbstractLogicalOperator aop = (AbstractLogicalOperator) opRef.getValue();
                if (aop.getOperatorTag() == LogicalOperatorTag.EXCHANGE)
                    aop = (AbstractLogicalOperator) aop.getInputs().get(0).getValue();
                if (aop.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE)
                    candidates.remove(i);
            }
        }
        for (int i = equivalenceClasses.size() - 1; i >= 0; i--)
            if (equivalenceClasses.get(i).size() < 2)
                equivalenceClasses.remove(i);
    }

    private boolean rewrite(IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        for (List<Mutable<ILogicalOperator>> members : equivalenceClasses) {
            if (rewriteForOneEquivalentClass(members, context))
                changed = true;
        }
        return changed;
    }

    private boolean rewriteForOneEquivalentClass(List<Mutable<ILogicalOperator>> members, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> group = new ArrayList<Mutable<ILogicalOperator>>();
        boolean rewritten = false;
        while (members.size() > 0) {
            group.clear();
            Mutable<ILogicalOperator> candidate = members.remove(members.size() - 1);
            group.add(candidate);
            for (int i = members.size() - 1; i >= 0; i--) {
                Mutable<ILogicalOperator> peer = members.get(i);
                if (IsomorphismUtilities.isOperatorIsomorphic(candidate.getValue(), peer.getValue())) {
                    group.add(peer);
                    members.remove(i);
                }
            }
            boolean[] materializationFlags = computeMaterilizationFlags(group);
            if (group.isEmpty()) {
                continue;
            }
            candidate = group.get(0);
            ReplicateOperator rop = new ReplicateOperator(group.size(), materializationFlags);
            rop.setPhysicalOperator(new ReplicatePOperator());
            rop.setExecutionMode(ExecutionMode.PARTITIONED);
            Mutable<ILogicalOperator> ropRef = new MutableObject<ILogicalOperator>(rop);
            AbstractLogicalOperator aopCandidate = (AbstractLogicalOperator) candidate.getValue();
            List<Mutable<ILogicalOperator>> originalCandidateParents = childrenToParents.get(candidate);

            if (aopCandidate.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                rop.getInputs().add(candidate);
            } else {
                AbstractLogicalOperator beforeExchange = new ExchangeOperator();
                beforeExchange.setPhysicalOperator(new OneToOneExchangePOperator());
                Mutable<ILogicalOperator> beforeExchangeRef = new MutableObject<ILogicalOperator>(beforeExchange);
                beforeExchange.getInputs().add(candidate);
                context.computeAndSetTypeEnvironmentForOperator(beforeExchange);
                rop.getInputs().add(beforeExchangeRef);
            }
            context.computeAndSetTypeEnvironmentForOperator(rop);

            for (Mutable<ILogicalOperator> parentRef : originalCandidateParents) {
                AbstractLogicalOperator parent = (AbstractLogicalOperator) parentRef.getValue();
                int index = parent.getInputs().indexOf(candidate);
                if (parent.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    parent.getInputs().set(index, ropRef);
                    rop.getOutputs().add(parentRef);
                } else {
                    AbstractLogicalOperator exchange = new ExchangeOperator();
                    exchange.setPhysicalOperator(new OneToOneExchangePOperator());
                    MutableObject<ILogicalOperator> exchangeRef = new MutableObject<ILogicalOperator>(exchange);
                    exchange.getInputs().add(ropRef);
                    rop.getOutputs().add(exchangeRef);
                    context.computeAndSetTypeEnvironmentForOperator(exchange);
                    parent.getInputs().set(index, exchangeRef);
                    context.computeAndSetTypeEnvironmentForOperator(parent);
                }
            }
            List<LogicalVariable> liveVarsNew = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(candidate.getValue(), liveVarsNew);
            ArrayList<Mutable<ILogicalExpression>> assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
            for (LogicalVariable liveVar : liveVarsNew)
                assignExprs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar)));
            for (Mutable<ILogicalOperator> ref : group) {
                if (ref.equals(candidate))
                    continue;
                ArrayList<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                Map<LogicalVariable, LogicalVariable> variableMappingBack = new HashMap<LogicalVariable, LogicalVariable>();
                IsomorphismUtilities.mapVariablesTopDown(ref.getValue(), candidate.getValue(), variableMappingBack);
                for (int i = 0; i < liveVarsNew.size(); i++) {
                    liveVars.add(variableMappingBack.get(liveVarsNew.get(i)));
                }

                AbstractLogicalOperator assignOperator = new AssignOperator(liveVars, assignExprs);
                assignOperator.setPhysicalOperator(new AssignPOperator());
                AbstractLogicalOperator projectOperator = new ProjectOperator(liveVars);
                projectOperator.setPhysicalOperator(new StreamProjectPOperator());
                AbstractLogicalOperator exchOp = new ExchangeOperator();
                exchOp.setPhysicalOperator(new OneToOneExchangePOperator());
                exchOp.getInputs().add(ropRef);
                MutableObject<ILogicalOperator> exchOpRef = new MutableObject<ILogicalOperator>(exchOp);
                rop.getOutputs().add(exchOpRef);
                assignOperator.getInputs().add(exchOpRef);
                projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

                // set the types
                context.computeAndSetTypeEnvironmentForOperator(exchOp);
                context.computeAndSetTypeEnvironmentForOperator(assignOperator);
                context.computeAndSetTypeEnvironmentForOperator(projectOperator);

                List<Mutable<ILogicalOperator>> parentOpList = childrenToParents.get(ref);
                for (Mutable<ILogicalOperator> parentOpRef : parentOpList) {
                    AbstractLogicalOperator parentOp = (AbstractLogicalOperator) parentOpRef.getValue();
                    int index = parentOp.getInputs().indexOf(ref);
                    if (parentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                        AbstractLogicalOperator parentOpNext = (AbstractLogicalOperator) childrenToParents
                                .get(parentOpRef).get(0).getValue();
                        if (parentOpNext.isMap()) {
                            index = parentOpNext.getInputs().indexOf(parentOpRef);
                            parentOp = parentOpNext;
                        }
                    }

                    ILogicalOperator childOp = parentOp.getOperatorTag() == LogicalOperatorTag.PROJECT ? assignOperator
                            : projectOperator;
                    if (parentOp.isMap()) {
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(childOp));
                    } else {
                        AbstractLogicalOperator exchg = new ExchangeOperator();
                        exchg.setPhysicalOperator(new OneToOneExchangePOperator());
                        exchg.getInputs().add(new MutableObject<ILogicalOperator>(childOp));
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(exchg));
                        context.computeAndSetTypeEnvironmentForOperator(exchg);
                    }
                    context.computeAndSetTypeEnvironmentForOperator(parentOp);
                }
            }
            rewritten = true;
        }
        return rewritten;
    }

    private void genCandidates(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();
        while (equivalenceClasses.size() > 0) {
            previousEquivalenceClasses.clear();
            for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
                List<Mutable<ILogicalOperator>> candidatesCopy = new ArrayList<Mutable<ILogicalOperator>>();
                candidatesCopy.addAll(candidates);
                previousEquivalenceClasses.add(candidatesCopy);
            }
            List<Mutable<ILogicalOperator>> currentLevelOpRefs = new ArrayList<Mutable<ILogicalOperator>>();
            for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
                if (candidates.size() > 0) {
                    for (Mutable<ILogicalOperator> opRef : candidates) {
                        List<Mutable<ILogicalOperator>> refs = childrenToParents.get(opRef);
                        if (refs != null)
                            currentLevelOpRefs.addAll(refs);
                    }
                }
                if (currentLevelOpRefs.size() == 0)
                    continue;
                candidatesGrow(currentLevelOpRefs, candidates);
            }
            if (currentLevelOpRefs.size() == 0)
                break;
            prune(context);
        }
        if (equivalenceClasses.size() < 1 && previousEquivalenceClasses.size() > 0) {
            equivalenceClasses.addAll(previousEquivalenceClasses);
            prune(context);
        }
    }

    private void topDownMaterialization(List<Mutable<ILogicalOperator>> tops) {
        List<Mutable<ILogicalOperator>> candidates = new ArrayList<Mutable<ILogicalOperator>>();
        List<Mutable<ILogicalOperator>> nextLevel = new ArrayList<Mutable<ILogicalOperator>>();
        for (Mutable<ILogicalOperator> op : tops) {
            for (Mutable<ILogicalOperator> opRef : op.getValue().getInputs()) {
                List<Mutable<ILogicalOperator>> opRefList = childrenToParents.get(opRef);
                if (opRefList == null) {
                    opRefList = new ArrayList<Mutable<ILogicalOperator>>();
                    childrenToParents.put(opRef, opRefList);
                    nextLevel.add(opRef);
                }
                opRefList.add(op);
            }
            if (op.getValue().getInputs().size() == 0)
                candidates.add(op);
        }
        if (equivalenceClasses.size() > 0) {
            equivalenceClasses.get(0).addAll(candidates);
        } else {
            equivalenceClasses.add(candidates);
        }
        if (nextLevel.size() > 0) {
            topDownMaterialization(nextLevel);
        }
    }

    private void candidatesGrow(List<Mutable<ILogicalOperator>> opList, List<Mutable<ILogicalOperator>> candidates) {
        List<Mutable<ILogicalOperator>> previousCandidates = new ArrayList<Mutable<ILogicalOperator>>();
        previousCandidates.addAll(candidates);
        candidates.clear();
        boolean validCandidate = false;
        for (Mutable<ILogicalOperator> op : opList) {
            List<Mutable<ILogicalOperator>> inputs = op.getValue().getInputs();
            for (int i = 0; i < inputs.size(); i++) {
                Mutable<ILogicalOperator> inputRef = inputs.get(i);
                validCandidate = false;
                for (Mutable<ILogicalOperator> candidate : previousCandidates) {
                    // if current input is in candidates
                    if (inputRef.getValue().equals(candidate.getValue())) {
                        if (inputs.size() == 1) {
                            validCandidate = true;
                        } else {
                            BitSet candidateInputBitMap = opToCandidateInputs.get(op);
                            if (candidateInputBitMap == null) {
                                candidateInputBitMap = new BitSet(inputs.size());
                                opToCandidateInputs.put(op, candidateInputBitMap);
                            }
                            candidateInputBitMap.set(i);
                            if (candidateInputBitMap.cardinality() == inputs.size()) {
                                validCandidate = true;
                            }
                        }
                        break;
                    }
                }
            }
            if (!validCandidate)
                continue;
            if (!candidates.contains(op))
                candidates.add(op);
        }
    }

    private void prune(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            List<Mutable<ILogicalOperator>> candidatesCopy = new ArrayList<Mutable<ILogicalOperator>>();
            candidatesCopy.addAll(candidates);
            previousEquivalenceClasses.add(candidatesCopy);
        }
        equivalenceClasses.clear();
        for (List<Mutable<ILogicalOperator>> candidates : previousEquivalenceClasses) {
            boolean[] reserved = new boolean[candidates.size()];
            for (int i = 0; i < reserved.length; i++)
                reserved[i] = false;
            for (int i = candidates.size() - 1; i >= 0; i--) {
                if (reserved[i] == false) {
                    List<Mutable<ILogicalOperator>> equivalentClass = new ArrayList<Mutable<ILogicalOperator>>();
                    ILogicalOperator candidate = candidates.get(i).getValue();
                    equivalentClass.add(candidates.get(i));
                    for (int j = i - 1; j >= 0; j--) {
                        ILogicalOperator peer = candidates.get(j).getValue();
                        if (IsomorphismUtilities.isOperatorIsomorphic(candidate, peer)) {
                            reserved[i] = true;
                            reserved[j] = true;
                            equivalentClass.add(candidates.get(j));
                        }
                    }
                    if (equivalentClass.size() > 1) {
                        equivalenceClasses.add(equivalentClass);
                        Collections.reverse(equivalentClass);
                    }
                }
            }
            for (int i = candidates.size() - 1; i >= 0; i--) {
                if (!reserved[i]) {
                    candidates.remove(i);
                }
            }
        }
    }

    private boolean[] computeMaterilizationFlags(List<Mutable<ILogicalOperator>> group) {
        lastUsedClusterId = 0;
        for (Mutable<ILogicalOperator> root : roots) {
            computeClusters(null, root, new MutableInt(++lastUsedClusterId));
        }
        boolean[] materializationFlags = new boolean[group.size()];
        boolean worthMaterialization = worthMaterialization(group.get(0));
        boolean requiresMaterialization;
        // get clusterIds for each candidate in the group
        List<Integer> groupClusterIds = new ArrayList<Integer>(group.size());
        for (int i = 0; i < group.size(); i++) {
            groupClusterIds.add(clusterMap.get(group.get(i)).getValue());
        }
        for (int i = group.size() - 1; i >= 0; i--) {
            requiresMaterialization = requiresMaterialization(groupClusterIds, i);
            if (requiresMaterialization && !worthMaterialization) {
                group.remove(i);
                groupClusterIds.remove(i);
            }
            materializationFlags[i] = requiresMaterialization;
        }
        if (group.size() < 2) {
            group.clear();
        }
        // if does not worth materialization, the flags for the remaining candidates should be false
        return worthMaterialization ? materializationFlags : new boolean[group.size()];
    }

    private boolean requiresMaterialization(List<Integer> groupClusterIds, int index) {
        Integer clusterId = groupClusterIds.get(index);
        BitSet blockingClusters = new BitSet();
        getAllBlockingClusterIds(clusterId, blockingClusters);
        if (!blockingClusters.isEmpty()) {
            for (int i = 0; i < groupClusterIds.size(); i++) {
                if (i == index) {
                    continue;
                }
                if (blockingClusters.get(groupClusterIds.get(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void getAllBlockingClusterIds(int clusterId, BitSet blockingClusters) {
        BitSet waitFor = clusterWaitForMap.get(clusterId);
        if (waitFor != null) {
            for (int i = waitFor.nextSetBit(0); i >= 0; i = waitFor.nextSetBit(i + 1)) {
                getAllBlockingClusterIds(i, blockingClusters);
            }
            blockingClusters.or(waitFor);
        }
    }

    private void computeClusters(Mutable<ILogicalOperator> parentRef, Mutable<ILogicalOperator> opRef,
            MutableInt currentClusterId) {
        // only replicate operator has multiple outputs
        int outputIndex = 0;
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE) {
            ReplicateOperator rop = (ReplicateOperator) opRef.getValue();
            List<Mutable<ILogicalOperator>> outputs = rop.getOutputs();
            for (outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
                if (outputs.get(outputIndex).equals(parentRef)) {
                    break;
                }
            }
        } else if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.SPLIT) {
            SplitOperator rop = (SplitOperator) opRef.getValue();
            List<Mutable<ILogicalOperator>> outputs = rop.getOutputs();
            for (outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
                if (outputs.get(outputIndex).equals(parentRef)) {
                    break;
                }
            }
        }
        AbstractLogicalOperator aop = (AbstractLogicalOperator) opRef.getValue();
        Pair<int[], int[]> labels = aop.getPhysicalOperator().getInputOutputDependencyLabels(opRef.getValue());
        List<Mutable<ILogicalOperator>> inputs = opRef.getValue().getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            Mutable<ILogicalOperator> inputRef = inputs.get(i);
            if (labels.second[outputIndex] == 1 && labels.first[i] == 0) { // 1 -> 0
                if (labels.second.length == 1) {
                    clusterMap.put(opRef, currentClusterId);
                    // start a new cluster
                    MutableInt newClusterId = new MutableInt(++lastUsedClusterId);
                    computeClusters(opRef, inputRef, newClusterId);
                    BitSet waitForList = clusterWaitForMap.get(currentClusterId.getValue());
                    if (waitForList == null) {
                        waitForList = new BitSet();
                        clusterWaitForMap.put(currentClusterId.getValue(), waitForList);
                    }
                    waitForList.set(newClusterId.getValue());
                }
            } else { // 0 -> 0 and 1 -> 1
                MutableInt prevClusterId = clusterMap.get(opRef);
                if (prevClusterId == null || prevClusterId.getValue().equals(currentClusterId.getValue())) {
                    clusterMap.put(opRef, currentClusterId);
                    computeClusters(opRef, inputRef, currentClusterId);
                } else {
                    // merge prevClusterId and currentClusterId: update all the map entries that has currentClusterId to prevClusterId
                    for (BitSet bs : clusterWaitForMap.values()) {
                        if (bs.get(currentClusterId.getValue())) {
                            bs.clear(currentClusterId.getValue());
                            bs.set(prevClusterId.getValue());
                        }
                    }
                    currentClusterId.setValue(prevClusterId.getValue());
                }
            }
        }
    }

    protected boolean worthMaterialization(Mutable<ILogicalOperator> candidate) {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) candidate.getValue();
        if (aop.getPhysicalOperator().expensiveThanMaterialization()) {
            return true;
        }
        List<Mutable<ILogicalOperator>> inputs = candidate.getValue().getInputs();
        for (Mutable<ILogicalOperator> inputRef : inputs) {
            if (worthMaterialization(inputRef)) {
                return true;
            }
        }
        return false;
    }
}
