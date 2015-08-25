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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.util.GroupVSizeFrame;

public class ExternalSortRunMerger {

    protected final IHyracksTaskContext ctx;
    protected final IFrameWriter writer;

    private final List<RunAndMaxFrameSizePair> runs;
    private final BitSet currentGenerationRunAvailable;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;
    private final RecordDescriptor recordDesc;
    private final int framesLimit;
    private final int MAX_FRAME_SIZE;
    private final ArrayList<IFrameReader> tempRuns;
    private final int topK;
    private List<GroupVSizeFrame> inFrames;
    private VSizeFrame outputFrame;
    private ISorter sorter;

    private static final Logger LOGGER = Logger.getLogger(ExternalSortRunMerger.class.getName());

    // Added to measure the execution time when the profiler setting is enabled
    protected ExecutionTimeStopWatch profilerSW;
    protected String nodeJobSignature;
    protected String taskId;

    public ExternalSortRunMerger(IHyracksTaskContext ctx, ISorter sorter, List<RunAndMaxFrameSizePair> runs,
            int[] sortFields, IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer,
            RecordDescriptor recordDesc, int framesLimit, IFrameWriter writer) {
        this(ctx, sorter, runs, sortFields, comparators, nmkComputer, recordDesc, framesLimit, Integer.MAX_VALUE,
                writer);
    }

    public ExternalSortRunMerger(IHyracksTaskContext ctx, ISorter sorter, List<RunAndMaxFrameSizePair> runs,
            int[] sortFields, IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer,
            RecordDescriptor recordDesc, int framesLimit, int topK, IFrameWriter writer) {
        this.ctx = ctx;
        this.sorter = sorter;
        this.runs = new LinkedList<>(runs);
        this.currentGenerationRunAvailable = new BitSet(runs.size());
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.nmkComputer = nmkComputer;
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
        this.MAX_FRAME_SIZE = FrameConstants.MAX_NUM_MINFRAME * ctx.getInitialFrameSize();
        this.topK = topK;
        this.tempRuns = new ArrayList<>(runs.size());
    }

    public void process() throws HyracksDataException {
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
            System.out.println("EXTERNAL_SORT_RUN_MERGER open() " + nodeJobSignature + " " + taskId);

            profilerSW.resume();
        }

        IFrameWriter finalWriter = null;
        try {
            if (runs.size() <= 0) {
                finalWriter = prepareSkipMergingFinalResultWriter(writer);
                finalWriter.open();
                if (sorter != null) {
                    if (sorter.hasRemaining()) {
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.suspend();
                        }
                        sorter.flush(finalWriter);
                        // Added to measure the execution time when the profiler setting is enabled
                        if (ExecutionTimeProfiler.PROFILE_MODE) {
                            profilerSW.resume();
                        }
                    }
                    sorter.close();
                    // Added to measure the execution time when the profiler setting is enabled
                    if (ExecutionTimeProfiler.PROFILE_MODE) {
                        profilerSW.suspend();
                    }
                }
            } else {
                /** recycle sort buffer */
                if (sorter != null) {
                    sorter.close();
                }

                finalWriter = prepareFinalMergeResultWriter(writer);
                finalWriter.open();

                int maxMergeWidth = framesLimit - 1;

                inFrames = new ArrayList<>(maxMergeWidth);
                outputFrame = new VSizeFrame(ctx);
                List<RunAndMaxFrameSizePair> partialRuns = new ArrayList<>(maxMergeWidth);

                int stop = runs.size();
                currentGenerationRunAvailable.set(0, stop);

                while (true) {

                    int unUsed = selectPartialRuns(maxMergeWidth * ctx.getInitialFrameSize(), runs, partialRuns,
                            currentGenerationRunAvailable, stop);
                    prepareFrames(unUsed, inFrames, partialRuns);

                    if (!currentGenerationRunAvailable.isEmpty() || stop < runs.size()) {
                        IFrameReader reader;
                        int mergedMaxFrameSize;
                        if (partialRuns.size() == 1) {
                            if (!currentGenerationRunAvailable.isEmpty()) {
                                throw new HyracksDataException(
                                        "The record is too big to put into the merging frame, please"
                                                + " allocate more sorting memory");
                            } else {
                                reader = partialRuns.get(0).run;
                                mergedMaxFrameSize = partialRuns.get(0).maxFrameSize;
                            }

                        } else {
                            RunFileWriter mergeFileWriter = prepareIntermediateMergeRunFile();
                            IFrameWriter mergeResultWriter = prepareIntermediateMergeResultWriter(mergeFileWriter);

                            mergeResultWriter.open();
                            mergedMaxFrameSize = merge(mergeResultWriter, partialRuns);
                            mergeResultWriter.close();

                            reader = mergeFileWriter.createReader();
                        }

                        appendNewRuns(reader, mergedMaxFrameSize);
                        if (currentGenerationRunAvailable.isEmpty()) {

                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("generated runs:" + stop);
                            }
                            runs.subList(0, stop).clear();
                            currentGenerationRunAvailable.clear();
                            currentGenerationRunAvailable.set(0, runs.size());
                            stop = runs.size();
                        }
                    } else {
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("final runs:" + stop);
                        }
                        merge(finalWriter, partialRuns);
                        break;
                    }
                }
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.suspend();
                }

            }
        } catch (Exception e) {
            finalWriter.fail();
            throw new HyracksDataException(e);
        } finally {
            finalWriter.close();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                profilerSW.finish();
                OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(nodeJobSignature, taskId, profilerSW
                        .getMessage("EXTERNAL_SORT_RUN_MERGER\t" + ctx.getTaskAttemptId() + "\t" + this.toString(),
                                profilerSW.getStartTimeStamp()), false);
                System.out.println("EXTERNAL_SORT_RUN_MERGER close() " + nodeJobSignature + " " + taskId);
            }
        }
    }

    private void appendNewRuns(IFrameReader reader, int mergedPartialMaxSize) {
        runs.add(new RunAndMaxFrameSizePair(reader, mergedPartialMaxSize));
    }

    private static int selectPartialRuns(int budget, List<RunAndMaxFrameSizePair> runs,
            List<RunAndMaxFrameSizePair> partialRuns, BitSet runAvailable, int stop) {
        partialRuns.clear();
        int maxFrameSizeOfGenRun = 0;
        int nextRunId = runAvailable.nextSetBit(0);
        while (budget > 0 && nextRunId >= 0 && nextRunId < stop) {
            int runFrameSize = runs.get(nextRunId).maxFrameSize;
            if (budget - runFrameSize >= 0) {
                partialRuns.add(runs.get(nextRunId));
                budget -= runFrameSize;
                runAvailable.clear(nextRunId);
                maxFrameSizeOfGenRun = runFrameSize > maxFrameSizeOfGenRun ? runFrameSize : maxFrameSizeOfGenRun;
            }
            nextRunId = runAvailable.nextSetBit(nextRunId + 1);
        }
        return budget;
    }

    private void prepareFrames(int extraFreeMem, List<GroupVSizeFrame> inFrames, List<RunAndMaxFrameSizePair> patialRuns)
            throws HyracksDataException {
        if (extraFreeMem > 0 && patialRuns.size() > 1) {
            int extraFrames = extraFreeMem / ctx.getInitialFrameSize();
            int avg = (extraFrames / patialRuns.size()) * ctx.getInitialFrameSize();
            int residue = (extraFrames % patialRuns.size());
            for (int i = 0; i < residue; i++) {
                patialRuns.get(i).updateSize(
                        Math.min(MAX_FRAME_SIZE, patialRuns.get(i).maxFrameSize + avg + ctx.getInitialFrameSize()));
            }
            for (int i = residue; i < patialRuns.size() && avg > 0; i++) {
                patialRuns.get(i).updateSize(Math.min(MAX_FRAME_SIZE, patialRuns.get(i).maxFrameSize + avg));
            }
        }

        if (inFrames.size() > patialRuns.size()) {
            inFrames.subList(patialRuns.size(), inFrames.size()).clear();
        }
        int i;
        for (i = 0; i < inFrames.size(); i++) {
            inFrames.get(i).resize(patialRuns.get(i).maxFrameSize);
        }
        for (; i < patialRuns.size(); i++) {
            inFrames.add(new GroupVSizeFrame(ctx, patialRuns.get(i).maxFrameSize));
        }
    }

    protected IFrameWriter prepareSkipMergingFinalResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        return nextWriter;
    }

    protected RunFileWriter prepareIntermediateMergeRunFile() throws HyracksDataException {
        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortRunMerger.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIOManager());
    }

    protected IFrameWriter prepareIntermediateMergeResultWriter(RunFileWriter mergeFileWriter)
            throws HyracksDataException {
        return mergeFileWriter;
    }

    protected IFrameWriter prepareFinalMergeResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        return nextWriter;
    }

    protected int[] getSortFields() {
        return sortFields;
    }

    private int merge(IFrameWriter writer, List<RunAndMaxFrameSizePair> partialRuns) throws HyracksDataException {
        tempRuns.clear();
        for (int i = 0; i < partialRuns.size(); i++) {
            tempRuns.add(partialRuns.get(i).run);
        }
        RunMergingFrameReader merger = new RunMergingFrameReader(ctx, tempRuns, inFrames, getSortFields(), comparators,
                nmkComputer, recordDesc, topK);
        int maxFrameSize = 0;
        int io = 0;
        merger.open();
        try {
            while (merger.nextFrame(outputFrame)) {
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.suspend();
                }
                FrameUtils.flushFrame(outputFrame.getBuffer(), writer);
                // Added to measure the execution time when the profiler setting is enabled
                if (ExecutionTimeProfiler.PROFILE_MODE) {
                    profilerSW.resume();
                }
                maxFrameSize = maxFrameSize < outputFrame.getFrameSize() ? outputFrame.getFrameSize() : maxFrameSize;
                io++;
            }
        } finally {
            merger.close();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Output " + io + " frames");
            }
        }
        return maxFrameSize;
    }

}
