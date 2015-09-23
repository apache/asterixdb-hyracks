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
package org.apache.hyracks.algebricks.runtime.operators.sort;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.sort.FrameSorterMergeSort;
import org.apache.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotLastFit;
import org.apache.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.sort.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;

public class InMemorySortRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int[] sortFields;
    private INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private IBinaryComparatorFactory[] comparatorFactories;

    public InMemorySortRuntimeFactory(int[] sortFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, int[] projectionList) {
        super(projectionList);
        // Obs: the projection list is currently ignored.
        if (projectionList != null) {
            throw new NotImplementedException("Cannot push projection into InMemorySortRuntime.");
        }
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputPushRuntime() {

            FrameSorterMergeSort frameSorter = null;

            @Override
            public void open() throws HyracksDataException {
                if (frameSorter == null) {
                    IFrameBufferManager manager = new VariableFrameMemoryManager(new VariableFramePool(ctx,
                            VariableFramePool.UNLIMITED_MEMORY), new FrameFreeSlotLastFit());
                    frameSorter = new FrameSorterMergeSort(ctx, manager, sortFields, firstKeyNormalizerFactory,
                            comparatorFactories, outputRecordDesc);
                }
                frameSorter.reset();
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameSorter.insertFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                frameSorter.sort();
                frameSorter.flush(writer);
                writer.close();
            }
        };
    }
}
