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

import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;

@SuppressWarnings("rawtypes")
public class DataSourceScanPOperator extends AbstractScanPOperator {

    private IDataSource<?> dataSource;
    private Object implConfig;

    public DataSourceScanPOperator(IDataSource<?> dataSource) {
        this.dataSource = dataSource;
    }

    public void setImplConfig(Object implConfig) {
        this.implConfig = implConfig;
    }

    public Object getImplConfig() {
        return implConfig;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.DATASOURCE_SCAN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        // partitioning properties
        DataSourceScanOperator dssOp = (DataSourceScanOperator) op;
        IDataSourcePropertiesProvider dspp = dataSource.getPropertiesProvider();
        deliveredProperties = dspp.computePropertiesVector(dssOp.getVariables());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        DataSourceScanOperator scan = (DataSourceScanOperator) op;
        IMetadataProvider mp = context.getMetadataProvider();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        List<LogicalVariable> vars = scan.getVariables();
        List<LogicalVariable> projectVars = scan.getProjectVariables();

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = mp.getScannerRuntime(dataSource, vars,
                projectVars, scan.isProjectPushed(), scan.getMinFilterVars(), scan.getMaxFilterVars(), opSchema,
                typeEnv, context, builder.getJobSpec(), implConfig);
        builder.contributeHyracksOperator(scan, p.first);
        if (p.second != null) {
            builder.contributeAlgebricksPartitionConstraint(p.first, p.second);
        }

        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }
}
