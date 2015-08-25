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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputSourcePushRuntime;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class EmptyTupleSourceRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    public EmptyTupleSourceRuntimeFactory() {
    }

    @Override
    public String toString() {
        return "ets";
    }

    @Override
    public IPushRuntime createPushRuntime(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new AbstractOneInputSourcePushRuntime() {

            private ArrayTupleBuilder tb = new ArrayTupleBuilder(0);
            private FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

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
                    System.out.println("EMPTY_TUPLE_SOURCE finish " + nodeJobSignature + " " + taskId);
                }

                writer.open();

                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.resume();
                }

                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }

                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.suspend();
                }

                appender.flush(writer, true);
                writer.close();
            }

            @Override
            public void close() throws HyracksDataException {
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(
                            nodeJobSignature,
                            taskId,
                            profilerSW.getMessage("EMPTY_TUPLE_SOURCE\t" + ctx.getTaskAttemptId(),
                                    profilerSW.getStartTimeStamp()), false);
                    System.out.println("EMPTY_TUPLE_SOURCE end " + nodeJobSignature + " " + taskId);
                }
            }

        };
    }
}
