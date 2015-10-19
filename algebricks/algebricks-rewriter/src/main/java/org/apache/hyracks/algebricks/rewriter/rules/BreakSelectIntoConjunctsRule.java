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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class BreakSelectIntoConjunctsRule implements IAlgebraicRewriteRule {

    private List<Mutable<ILogicalExpression>> conjs = new ArrayList<Mutable<ILogicalExpression>>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;

        ILogicalExpression cond = select.getCondition().getValue();

        conjs.clear();
        if (!cond.splitIntoConjuncts(conjs)) {
            return false;
        }

        Mutable<ILogicalOperator> childOfSelect = select.getInputs().get(0);
        boolean fst = true;
        ILogicalOperator botOp = select;
        ILogicalExpression firstExpr = null;
        for (Mutable<ILogicalExpression> eRef : conjs) {
            ILogicalExpression e = eRef.getValue();
            if (fst) {
                fst = false;
                firstExpr = e;
            } else {
                SelectOperator newSelect = new SelectOperator(new MutableObject<ILogicalExpression>(e),
                        select.getRetainNull(), select.getNullPlaceholderVariable());
                List<Mutable<ILogicalOperator>> botInpList = botOp.getInputs();
                botInpList.clear();
                botInpList.add(new MutableObject<ILogicalOperator>(newSelect));
                context.computeAndSetTypeEnvironmentForOperator(botOp);
                botOp = newSelect;
            }
        }
        botOp.getInputs().add(childOfSelect);
        select.getCondition().setValue(firstExpr);
        context.computeAndSetTypeEnvironmentForOperator(botOp);
        context.computeAndSetTypeEnvironmentForOperator(select);

        return true;
    }
}
