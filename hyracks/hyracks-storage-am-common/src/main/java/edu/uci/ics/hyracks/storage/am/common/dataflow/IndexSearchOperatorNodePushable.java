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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
import edu.uci.ics.hyracks.api.util.OperatorExecutionTimeProfiler;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public abstract class IndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected FrameTupleAccessor accessor;

    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected IIndex index;
    protected ISearchPredicate searchPred;
    protected IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    protected final RecordDescriptor inputRecDesc;
    protected final RecordDescriptor outputRecDesc;
    protected final boolean retainInput;
    protected FrameTupleReference frameTuple;

    protected final boolean retainNull;
    protected ArrayTupleBuilder nullTupleBuild;
    protected INullWriter nullWriter;

    protected final int[] minFilterFieldIndexes;
    protected final int[] maxFilterFieldIndexes;
    protected PermutingFrameTupleReference minFilterKey;
    protected PermutingFrameTupleReference maxFilterKey;

    // Added to measure the execution time when the profiler setting is enabled
    protected ExecutionTimeStopWatch profilerSW;
    protected String nodeJobSignature;
    protected String taskId;
    protected String indexType;

    public IndexSearchOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IRecordDescriptorProvider recordDescProvider, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.retainInput = opDesc.getRetainInput();
        this.retainNull = opDesc.getRetainNull();
        if (this.retainNull) {
            this.nullWriter = opDesc.getNullWriterFactory().createNullWriter();
        }
        this.inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        this.outputRecDesc = recordDescProvider.getOutputRecordDescriptor(opDesc.getActivityId(), 0);
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        if (minFilterFieldIndexes != null && minFilterFieldIndexes.length > 0) {
            minFilterKey = new PermutingFrameTupleReference();
            minFilterKey.setFieldPermutation(minFilterFieldIndexes);
        }
        if (maxFilterFieldIndexes != null && maxFilterFieldIndexes.length > 0) {
            maxFilterKey = new PermutingFrameTupleReference();
            maxFilterKey.setFieldPermutation(maxFilterFieldIndexes);
        }
    }

    protected abstract ISearchPredicate createSearchPredicate();

    protected abstract void resetSearchPredicate(int tupleIndex);

    protected IIndexCursor createCursor() {
        return indexAccessor.createSearchCursor(false, opDesc.getUseOpercationCallbackProceedReturnResult(),
                opDesc.getRecordDescriptor(), opDesc.getValuesForOpercationCallbackProceedReturnResult());
    }

    protected abstract int getFieldCount();

    @Override
    public void open() throws HyracksDataException {
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        searchPred = createSearchPredicate();

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
            indexType = searchPred.applicableIndexType();

            // Check whether we are dealing with the primary index or secondary index
            if (indexType.equals("BTREE_INDEX")) {
                if (!indexHelper.needKeyDuplicateCheck()) {
                    indexType = "BTREE_PRIMARY_INDEX";
                } else {
                    indexType = "BTREE_SECONDARY_INDEX";
                }
            }
            System.out.println(indexType + "_SEARCH start " + nodeJobSignature + " " + taskId);
        }

        accessor = new FrameTupleAccessor(inputRecDesc);
        writer.open();

        if (retainNull) {
            int fieldCount = getFieldCount();
            nullTupleBuild = new ArrayTupleBuilder(fieldCount);
            DataOutput out = nullTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCount; i++) {
                try {
                    nullWriter.writeNull(out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                nullTupleBuild.addFieldEndOffset();
            }
        } else {
            nullTupleBuild = null;
        }

        try {
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            System.out.println("recordDesc.getFieldCount()" + recordDesc.getFieldCount() + " out field count: "
                    + outputRecDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory()
                    .createSearchOperationCallback(indexHelper.getResourceID(), ctx);
            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, searchCallback);
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    protected void writeSearchResults(int tupleIndex) throws Exception {
        boolean matched = false;
        while (cursor.hasNext()) {
            matched = true;
            tb.reset();
            cursor.next();
            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                    //                    ByteArrayInputStream inStreamZero = new ByteArrayInputStream(frameTuple.getFieldData(i),
                    //                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    //                    Object test = inputRecDesc.getFields()[i].deserialize(new DataInputStream(inStreamZero));
                    //                    System.out.println("input " + test + " " + opDesc.getActivityId());
                }

            }
            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
                //                ByteArrayInputStream inStreamZero = new ByteArrayInputStream(tuple.getFieldData(i),
                //                        tuple.getFieldStart(i), tuple.getFieldLength(i));
                //                int fieldNo = i;
                //                if (retainInput) {
                //                    fieldNo += frameTuple.getFieldCount();
                //                }
                //                Object test = outputRecDesc.getFields()[fieldNo].deserialize(new DataInputStream(inStreamZero));
                //                System.out.println(test + " " + opDesc.getActivityId());
            }
            // Added to measure the execution time when the profiler setting is enabled
            if (!ExecutionTimeProfiler.PROFILE_MODE) {
                FrameUtils
                        .appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            } else {
                // If the profiler is ON, then we use stopwatch to measure the time.
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize(), profilerSW);
            }
        }

        if (!matched && retainInput && retainNull) {
            if (!ExecutionTimeProfiler.PROFILE_MODE) {
                FrameUtils
                        .appendConcatToWriter(writer, appender, accessor, tupleIndex,
                                nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                                nullTupleBuild.getSize());
            } else {
                FrameUtils.appendConcatToWriter(writer, appender, accessor, tupleIndex,
                        nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                        nullTupleBuild.getSize(), profilerSW);
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.resume();
        }

        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                resetSearchPredicate(i);
                cursor.reset();
                indexAccessor.search(cursor, searchPred);
                writeSearchResults(i);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        // Added to measure the execution time when the profiler setting is enabled
        if (ExecutionTimeProfiler.PROFILE_MODE) {
            profilerSW.suspend();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            appender.flush(writer, true);
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            writer.close();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                profilerSW.finish();

                OperatorExecutionTimeProfiler.INSTANCE.executionTimeProfiler.add(
                        nodeJobSignature,
                        taskId,
                        profilerSW.getMessage(searchPred.applicableIndexType() + "_SEARCH\t" + ctx.getTaskAttemptId()
                                + "\t" + this.toString(), profilerSW.getStartTimeStamp()), false);
                System.out.println(indexType + "_SEARCH close() " + nodeJobSignature + " " + taskId);
            }
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
