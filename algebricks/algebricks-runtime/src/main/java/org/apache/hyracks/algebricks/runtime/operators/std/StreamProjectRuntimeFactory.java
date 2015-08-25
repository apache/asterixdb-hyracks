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

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;

public class StreamProjectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final boolean flushFramesRapidly;

    public StreamProjectRuntimeFactory(int[] projectionList, boolean flushFramesRapidly) {
        super(projectionList);
        this.flushFramesRapidly = flushFramesRapidly;
    }

    public StreamProjectRuntimeFactory(int[] projectionList) {
        this(projectionList, false);
    }

    @Override
    public String toString() {
        return "stream-project " + Arrays.toString(projectionList);
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private boolean first = true;

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
                    System.out.println("STREAM_PROJECT open() " + nodeJobSignature + " " + taskId);
                }

                if (first) {
                    first = false;
                    initAccessAppend(ctx);
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.resume();
                }

                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();

                int t = 0;
                if (nTuple > 1) {
                    if (!ExecutionTimeProfiler.PROFILE_MODE) {
                        for (; t < nTuple - 1; t++) {
                            appendProjectionToFrame(t, projectionList);
                        }
                    } else {
                        // Added to measure the execution time when the profiler setting is enabled
                        for (; t < nTuple - 1; t++) {
                            appendProjectionToFrame(t, projectionList, profilerSW);
                        }
                    }
                }
                if (flushFramesRapidly) {
                    // Whenever all the tuples in the incoming frame have been consumed, the project operator
                    // will push its frame to the next operator; i.e., it won't wait until the frame gets full.
                    if (!ExecutionTimeProfiler.PROFILE_MODE) {
                        appendProjectionToFrame(t, projectionList, true);
                    } else {
                        appendProjectionToFrame(t, projectionList, true, profilerSW);
                    }
                } else {
                    if (!ExecutionTimeProfiler.PROFILE_MODE) {
                        appendProjectionToFrame(t, projectionList);
                    } else {
                        appendProjectionToFrame(t, projectionList, profilerSW);
                    }
                }

            }

            @Override
            public void close() throws HyracksDataException {
                if (!ExecutionTimeProfiler.PROFILE_MODE) {
                    flushIfNotFailed();
                } else {
                    flushIfNotFailed(profilerSW);
                }
                writer.close();

                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.finish();
                    OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId,
                            profilerSW.getMessage("STREAM_PROJECT\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                    profilerSW.getStartTimeStamp()), false);
                    System.out.println("STREAM_PROJECT close() " + nodeJobSignature + " " + taskId);
                }

            }

        };
    }
}
