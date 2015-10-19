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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.JobSpecification;

public abstract class AbstractPhysicalOperator implements IPhysicalOperator {

    protected IPhysicalPropertiesVector deliveredProperties;
    private boolean disableJobGenBelow = false;
    private Object hostQueryContext;

    @Override
    public final IPhysicalPropertiesVector getDeliveredProperties() {
        return deliveredProperties;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString();
    }

    public void setHostQueryContext(Object context) {
        this.hostQueryContext = context;
    }

    public Object getHostQueryContext() {
        return hostQueryContext;
    }

    protected PhysicalRequirements emptyUnaryRequirements() {
        StructuralPropertiesVector[] req = new StructuralPropertiesVector[] { StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR };
        return new PhysicalRequirements(req, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    protected PhysicalRequirements emptyUnaryRequirements(int numberOfChildren) {
        StructuralPropertiesVector[] req = new StructuralPropertiesVector[numberOfChildren];
        for (int i = 0; i < numberOfChildren; i++) {
            req[i] = StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
        }
        return new PhysicalRequirements(req, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void disableJobGenBelowMe() {
        this.disableJobGenBelow = true;
    }

    @Override
    public boolean isJobGenDisabledBelowMe() {
        return disableJobGenBelow;
    }

    /**
     * @return labels (0 or 1) for each input and output indicating the dependency between them.
     *         The edges labeled as 1 must wait for the edges with label 0.
     */
    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        int[] inputDependencyLabels = new int[op.getInputs().size()]; // filled with 0's
        int[] outputDependencyLabels = new int[] { 0 };
        return new Pair<int[], int[]>(inputDependencyLabels, outputDependencyLabels);
    }

    protected void contributeOpDesc(IHyracksJobBuilder builder, AbstractLogicalOperator op, IOperatorDescriptor opDesc) {
        if (op.getExecutionMode() == ExecutionMode.UNPARTITIONED) {
            AlgebricksPartitionConstraint apc = new AlgebricksCountPartitionConstraint(1);
            builder.contributeAlgebricksPartitionConstraint(opDesc, apc);
        }
        builder.contributeHyracksOperator(op, opDesc);
    }

    protected AlgebricksPipeline[] compileSubplans(IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        AlgebricksPipeline[] subplans = new AlgebricksPipeline[npOp.getNestedPlans().size()];
        PlanCompiler pc = new PlanCompiler(context);
        int i = 0;
        for (ILogicalPlan p : npOp.getNestedPlans()) {
            subplans[i++] = buildPipelineWithProjection(p, outerPlanSchema, npOp, opSchema, pc);
        }
        return subplans;
    }

    private AlgebricksPipeline buildPipelineWithProjection(ILogicalPlan p, IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, PlanCompiler pc) throws AlgebricksException {
        if (p.getRoots().size() > 1) {
            throw new NotImplementedException("Nested plans with several roots are not supported.");
        }
        JobSpecification nestedJob = pc.compilePlan(p, outerPlanSchema, null);
        ILogicalOperator topOpInSubplan = p.getRoots().get(0).getValue();
        JobGenContext context = pc.getContext();
        IOperatorSchema topOpInSubplanScm = context.getSchema(topOpInSubplan);
        opSchema.addAllVariables(topOpInSubplanScm);

        Map<OperatorDescriptorId, IOperatorDescriptor> opMap = nestedJob.getOperatorMap();
        if (opMap.size() != 1) {
            throw new AlgebricksException(
                    "Attempting to construct a nested plan with "
                            + opMap.size()
                            + " operator descriptors. Currently, nested plans can only consist in linear pipelines of Asterix micro operators.");
        }

        for (OperatorDescriptorId oid : opMap.keySet()) {
            IOperatorDescriptor opd = opMap.get(oid);
            if (!(opd instanceof AlgebricksMetaOperatorDescriptor)) {
                throw new AlgebricksException(
                        "Can only generate Hyracks jobs for pipelinable Asterix nested plans, not for "
                                + opd.getClass().getName());
            }
            AlgebricksMetaOperatorDescriptor amod = (AlgebricksMetaOperatorDescriptor) opd;

            return amod.getPipeline();
            // we suppose that the top operator in the subplan already does the
            // projection for us
        }

        throw new IllegalStateException();
    }
}
