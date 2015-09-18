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
package org.apache.hyracks.dataflow.std.union;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.util.ExecutionTimeProfiler;
import org.apache.hyracks.api.util.ExecutionTimeStopWatch;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;

public class UnionAllOperatorDescriptor extends AbstractOperatorDescriptor {
    public UnionAllOperatorDescriptor(IOperatorDescriptorRegistry spec, int nInputs, RecordDescriptor recordDescriptor) {
        super(spec, nInputs, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        UnionActivityNode uba = new UnionActivityNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, uba);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, uba, i);
        }
        builder.addTargetEdge(0, uba, 0);
    }

    private class UnionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public UnionActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new UnionOperator(ctx, inRecordDesc);
        }
    }

    private class UnionOperator extends AbstractUnaryOutputOperatorNodePushable {
        private int nOpened;

        private int nClosed;

        private boolean failed;

        // Originally, this variable is not required for this class.
        // However, this variable was added to measure the execution time when the profiler setting is enabled.
        IHyracksTaskContext ctx;

        public UnionOperator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc) {
            nOpened = 0;
            nClosed = 0;
            this.ctx = ctx;
        }

        @Override
        public int getInputArity() {
            return inputArity;
        }

        @Override
        public IFrameWriter getInputFrameWriter(int index) {
            return new IFrameWriter() {

                // Added to measure the execution time when the profiler setting is enabled
                private ExecutionTimeStopWatch profilerSW;
                private String nodeJobSignature;
                private String taskId;

                @Override
                public void open() throws HyracksDataException {
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW = new ExecutionTimeStopWatch();
                        profilerSW.start();

                        // The key of this job: nodeId + JobId + Joblet hash code
                        nodeJobSignature = ctx.getJobletContext().getApplicationContext().getNodeId() + "_"
                                + ctx.getJobletContext().getJobId() + "_" + ctx.getJobletContext().hashCode();

                        // taskId: partition + taskId + started time
                        taskId = ctx.getTaskAttemptId() + this.toString() + profilerSW.getStartTimeStamp();

                        // Initialize the counter for this runtime instance
                        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                                ExecutionTimeProfiler.INIT, false);
                        System.out.println("UNION_ALL open() " + nodeJobSignature + " " + taskId);
                    }

                    synchronized (UnionOperator.this) {
                        if (++nOpened == 1) {
                            writer.open();
                        }
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.resume();
                    }

                    synchronized (UnionOperator.this) {
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.suspend();
                        }
                        writer.nextFrame(buffer);
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.resume();
                        }
                    }
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.suspend();
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (failed) {
                            writer.fail();
                        }
                        failed = true;
                    }
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.finish();
                        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                                profilerSW.getMessage("UNION_ALL\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                        profilerSW.getStartTimeStamp()), false);
                        System.out.println("UNION_ALL fail() " + nodeJobSignature + " " + taskId);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (++nClosed == inputArity) {
                            writer.close();
                        }
                    }
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.finish();
                        OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                                profilerSW.getMessage("UNION_ALL\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                        profilerSW.getStartTimeStamp()), false);
                        System.out.println("UNION_ALL close() " + nodeJobSignature + " " + taskId);
                    }

                }
            };
        }
    }
}