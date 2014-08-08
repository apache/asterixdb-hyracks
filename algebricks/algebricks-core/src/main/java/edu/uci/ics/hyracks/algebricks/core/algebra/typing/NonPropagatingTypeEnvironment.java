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
package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonPropagatingTypeEnvironment extends AbstractTypeEnvironment {

    public NonPropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMetadataProvider<?, ?> metadataProvider) {
        super(expressionTypeComputer, metadataProvider);
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return varTypeMap.get(var);
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        return getVarType(var);
    }

}
