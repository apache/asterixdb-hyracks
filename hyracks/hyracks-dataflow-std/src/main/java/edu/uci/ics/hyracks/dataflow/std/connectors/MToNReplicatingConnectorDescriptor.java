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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.nio.ByteBuffer;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;

public class MToNReplicatingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    public MToNReplicatingConnectorDescriptor(IConnectorDescriptorRegistry spec) {
        super(spec);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            epWriters[i] = edwFactory.createFrameWriter(i);
        }

        // For Experiment Profiler
        final String nodeJobSignature = ctx.getJobletContext().getApplicationContext().getNodeId()
                + ctx.getJobletContext().getJobId() + ctx.getJobletContext().hashCode();
        final String taskId = ctx.getTaskAttemptId() + this.toString();

        return new IFrameWriter() {

            // For Experiment Profiler
            private ExecutionTimeStopWatch profilerSW;
            private String taskIdWithTimeStamp;

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // For Experiment Profiler
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.resume();
                }
                buffer.mark();
                for (int i = 0; i < epWriters.length; ++i) {
                    if (i != 0) {
                        buffer.reset();
                    }
                    epWriters[i].nextFrame(buffer);
                }
                // For Experiment Profiler
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.suspend();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // For Experiment Profiler
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature,
                            taskIdWithTimeStamp,
                            profilerSW.getMessage("MToNReplicatingConnector fail", profilerSW.getStartTimeStamp()),
                            false);
                }

                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].fail();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].close();
                }
                // For Experiment Profiler
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature,
                            taskIdWithTimeStamp,
                            profilerSW.getMessage("MToNReplicatingConnector", profilerSW.getStartTimeStamp()), false);
                    System.out.println("MToNReplicatingConnector end " + taskIdWithTimeStamp);

                }
            }

            @Override
            public void open() throws HyracksDataException {
                // For Experiment Profiler
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW = new ExecutionTimeStopWatch();
                    profilerSW.start();
                    taskIdWithTimeStamp = taskId + profilerSW.getStartTimeStamp();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature,
                            taskIdWithTimeStamp, "init", false);
                    System.out.println("MToNReplicatingConnector start " + taskIdWithTimeStamp);
                }
                for (int i = 0; i < epWriters.length; ++i) {
                    epWriters[i].open();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(nProducerPartitions,
                expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, frameReader, channelReader);
    }
}