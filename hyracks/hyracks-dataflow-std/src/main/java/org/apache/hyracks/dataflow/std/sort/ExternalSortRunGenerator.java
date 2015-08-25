/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.EnumFreeSlotPolicy;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotBiggestFirst;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotLastFit;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotSmallestFit;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFrameFreeSlotPolicy;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFrameMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;

public class ExternalSortRunGenerator extends AbstractSortRunGenerator {

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;

    // Added to measure the execution time when the profiler setting is enabled
    protected ExecutionTimeStopWatch profilerSW;
    protected String nodeJobSignature;
    protected String taskId;

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg,
                EnumFreeSlotPolicy.LAST_FIT, framesLimit);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit)
            throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit)
            throws HyracksDataException {
        this.ctx = ctx;
        maxSortFrames = framesLimit - 1;

        IFrameFreeSlotPolicy freeSlotPolicy = null;
        switch (policy) {
            case SMALLEST_FIT:
                freeSlotPolicy = new FrameFreeSlotSmallestFit();
                break;
            case LAST_FIT:
                freeSlotPolicy = new FrameFreeSlotLastFit(maxSortFrames);
                break;
            case BIGGEST_FIT:
                freeSlotPolicy = new FrameFreeSlotBiggestFirst(maxSortFrames);
                break;
        }
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(new VariableFramePool(ctx, maxSortFrames
                * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, sortFields, firstKeyNormalizerFactory,
                    comparatorFactories, recordDesc, outputLimit);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, sortFields, firstKeyNormalizerFactory,
                    comparatorFactories, recordDesc, outputLimit);
        }
    }

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
            System.out.println("EXTERNAL_SORT_RUN_GENERATOR open() " + nodeJobSignature + " " + taskId);
        }
        runAndMaxSizes.clear();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.resume();
        }

        if (!frameSorter.insertFrame(buffer)) {
            flushFramesToRun();
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }

        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.suspend();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.resume();
        }
        if (getSorter().hasRemaining()) {
            if (runAndMaxSizes.size() <= 0) {
                getSorter().sort();
            } else {
                flushFramesToRun();
            }
        }
        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.suspend();
            profilerSW.finish();
            OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId, profilerSW
                    .getMessage("EXTERNAL_SORT_RUN_GENERATOR\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                            profilerSW.getStartTimeStamp()), false);
            System.out.println("EXTERNAL_SORT_RUN_GENERATOR close() " + nodeJobSignature + " " + taskId);
        }
    }

    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                ExternalSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        return writer;
    }

    @Override
    public ISorter getSorter() {
        return frameSorter;
    }

    @Override
    protected void flushFramesToRun() throws HyracksDataException {
        getSorter().sort();
        RunFileWriter runWriter = getRunFileWriter();
        IFrameWriter flushWriter = getFlushableFrameWriter(runWriter);
        flushWriter.open();
        int maxFlushedFrameSize;
        try {

            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                profilerSW.suspend();
            }
            maxFlushedFrameSize = getSorter().flush(flushWriter);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                profilerSW.resume();
            }
        } finally {
            flushWriter.close();
        }
        runAndMaxSizes.add(new RunAndMaxFrameSizePair(runWriter.createReader(), maxFlushedFrameSize));
        getSorter().reset();
    }

}