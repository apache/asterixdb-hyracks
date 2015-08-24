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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public interface IPhysicalOperator {

    public PhysicalOperatorTag getOperatorTag();

    /**
     * @param op
     *            the logical operator this physical operator annotates
     * @param reqdByParent
     *            parent's requirements, which are not enforced for now, as we
     *            only explore one plan
     * @return for each child, one vector of required physical properties
     */
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent);

    /**
     * @return the physical properties that this operator delivers, based on
     *         what its children deliver
     */
    public IPhysicalPropertiesVector getDeliveredProperties();

    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException;

    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException;

    public void disableJobGenBelowMe();

    public boolean isJobGenDisabledBelowMe();

    public boolean isMicroOperator();

    public void setHostQueryContext(Object context);

    public Object getHostQueryContext();

    /**
     * @return labels (0 or 1) for each input and output indicating the dependency between them.
     *         The edges labeled as 1 must wait for the edges with label 0.
     */
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op);

    /*
     * This is needed to have a kind of cost based decision on whether to merge the shared subplans and materialize the result.
     * If the subgraph whose result we would like to materialize has an operator that is computationally expensive, we assume
     * it is cheaper to materialize the result of this subgraph and read from the file rather than recomputing it.
     */
    public boolean expensiveThanMaterialization();

}