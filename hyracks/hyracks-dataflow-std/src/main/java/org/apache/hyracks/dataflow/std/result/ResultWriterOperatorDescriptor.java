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
package org.apache.hyracks.dataflow.std.result;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.util.ExecutionTimeProfiler;
import org.apache.hyracks.api.util.ExecutionTimeStopWatch;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;
import org.apache.hyracks.dataflow.common.comm.io.FrameOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ResultWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final ResultSetId rsId;

    private final boolean ordered;

    private final boolean asyncMode;

    private final IResultSerializerFactory resultSerializerFactory;

    public ResultWriterOperatorDescriptor(IOperatorDescriptorRegistry spec, ResultSetId rsId, boolean ordered,
            boolean asyncMode, IResultSerializerFactory resultSerializerFactory) throws IOException {
        super(spec, 1, 0);
        this.rsId = rsId;
        this.ordered = ordered;
        this.asyncMode = asyncMode;
        this.resultSerializerFactory = resultSerializerFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final IDatasetPartitionManager dpm = ctx.getDatasetPartitionManager();

        final IFrame frame = new VSizeFrame(ctx);

        final FrameOutputStream frameOutputStream = new FrameOutputStream(ctx.getInitialFrameSize());
        frameOutputStream.reset(frame, true);
        PrintStream printStream = new PrintStream(frameOutputStream);

        final RecordDescriptor outRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        final IResultSerializer resultSerializer = resultSerializerFactory.createResultSerializer(outRecordDesc,
                printStream);

        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(outRecordDesc);

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            IFrameWriter datasetPartitionWriter;

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
                    System.out.println("DISTRIBUTE_RESULT open() " + nodeJobSignature + " " + taskId);
                }

                try {
                    datasetPartitionWriter = dpm.createDatasetPartitionWriter(ctx, rsId, ordered, asyncMode, partition,
                            nPartitions);
                    datasetPartitionWriter.open();
                    resultSerializer.init();
                } catch (HyracksException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.resume();
                }

                frameTupleAccessor.reset(buffer);
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    resultSerializer.appendTuple(frameTupleAccessor, tIndex);
                    if (!frameOutputStream.appendTuple()) {
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.suspend();
                        }
                        frameOutputStream.flush(datasetPartitionWriter);
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.resume();
                        }

                        resultSerializer.appendTuple(frameTupleAccessor, tIndex);
                        frameOutputStream.appendTuple();
                    }
                }

                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.suspend();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                datasetPartitionWriter.fail();
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(
                            nodeJobSignature,
                            taskId,
                            profilerSW.getMessage(
                                    "DISTRIBUTE_RESULT\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                    profilerSW.getStartTimeStamp()), false);
                    System.out.println("DISTRIBUTE_RESULT fail() " + nodeJobSignature + " " + taskId);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (frameOutputStream.getTupleCount() > 0) {
                    frameOutputStream.flush(datasetPartitionWriter);
                }
                datasetPartitionWriter.close();
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(
                            nodeJobSignature,
                            taskId,
                            profilerSW.getMessage(
                                    "DISTRIBUTE_RESULT\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                    profilerSW.getStartTimeStamp()), false);
                    System.out.println("DISTRIBUTE_RESULT close() " + nodeJobSignature + " " + taskId);
                }

            }
        };
    }
}