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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public interface ILogicalOperator {

    public LogicalOperatorTag getOperatorTag();

    public ExecutionMode getExecutionMode();

    public List<Mutable<ILogicalOperator>> getInputs();

    boolean hasInputs();

    public void recomputeSchema() throws AlgebricksException;

    public List<LogicalVariable> getSchema();

    /*
     *
     * support for visitors
     */

    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException;

    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException;

    public boolean isMap();

    public Map<String, Object> getAnnotations();

    public void removeAnnotation(String annotationName);

    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException;

    // variables

    /**
     * Get the variable propogation policy from this operator's input to its
     * output.
     *
     * @return The VariablePropogationPolicy.
     */
    public VariablePropagationPolicy getVariablePropagationPolicy();

    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException;

    public IVariableTypeEnvironment computeInputTypeEnvironment(ITypingContext ctx) throws AlgebricksException;

    // structural properties

    /**
     * @return for each child, one vector of required physical properties
     */

    public PhysicalRequirements getRequiredPhysicalPropertiesForChildren(IPhysicalPropertiesVector requiredProperties);

    /**
     * @return the physical properties that this operator delivers, based on
     *         what its children deliver
     */

    public IPhysicalPropertiesVector getDeliveredPhysicalProperties();

    public void computeDeliveredPhysicalProperties(IOptimizationContext context) throws AlgebricksException;

    /**
     * Indicates whether the expressions used by this operator must be variable reference expressions.
     */
    public boolean requiresVariableReferenceExpressions();
}