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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Split Operator receives a variable whose type is INT32 as conditional variable.
 * Based on its value, it propagates each tuple to the corresponding frame.
 * Thus, unlike Replicate operator that does unconditional propagation to all outputs,
 * this does a conditional propagate operation.
 * 
 * @author waans11
 */
public class SplitOperator extends AbstractLogicalOperator {

    private int outputArity = 2;
    private boolean[] outputMaterializationFlags = new boolean[outputArity];
    private List<Mutable<ILogicalOperator>> outputs;
    private LogicalVariable conditionVar;

    public SplitOperator(int outputArity, LogicalVariable conditionVar) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = new boolean[outputArity];
        this.outputs = new ArrayList<Mutable<ILogicalOperator>>();
        this.conditionVar = conditionVar;
    }

    public SplitOperator(int outputArity, boolean[] outputMaterializationFlags, LogicalVariable conditionVar) {
        this.outputArity = outputArity;
        this.outputMaterializationFlags = outputMaterializationFlags;
        this.outputs = new ArrayList<Mutable<ILogicalOperator>>();
        this.conditionVar = conditionVar;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SPLIT;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSplitOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
    }

    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        // do nothing
    }

    public int getOutputArity() {
        return outputArity;
    }

    public int setOutputArity(int outputArity) {
        return this.outputArity = outputArity;
    }

    public void setOutputMaterializationFlags(boolean[] outputMaterializationFlags) {
        this.outputMaterializationFlags = outputMaterializationFlags;
    }

    public boolean[] getOutputMaterializationFlags() {
        return outputMaterializationFlags;
    }

    public List<Mutable<ILogicalOperator>> getOutputs() {
        return outputs;
    }

    public LogicalVariable getConditionVar() {
        return conditionVar;
    }

    public int getConditionVarFieldPos() throws AlgebricksException {
        if (schema != null) {
            return schema.indexOf(conditionVar);
        } else {
            throw new AlgebricksException("SPLIT operator: can't get the position of conditional variable.");
        }
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    public boolean isBlocker() {
        for (boolean requiresMaterialization : outputMaterializationFlags) {
            if (requiresMaterialization) {
                return true;
            }
        }
        return false;
    }
}
