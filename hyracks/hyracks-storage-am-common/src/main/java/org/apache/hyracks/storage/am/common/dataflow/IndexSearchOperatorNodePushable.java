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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExecutionTimeProfiler;
import org.apache.hyracks.api.util.ExecutionTimeStopWatch;
import org.apache.hyracks.api.util.OperatorExecutionTimeProfiler;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.hdfs.lib.RawBinaryComparatorFactory;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

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

    // Need to keep the result of search operation callback result?
    protected boolean useOpercationCallbackProceedReturnResult = false;
    protected byte[] valuesForOpercationCallbackProceedReturnResult = null;

    // When useOpercationCallbackProceedReturnResult is true, and
    // this variable is set to a non-negative number, an index-search only generates the given number of results.
    protected long limitNumberOfResult = -1;
    protected boolean useLimitNumberOfResult = false;

    protected long searchedTupleCount = 0;
    protected IBinaryComparator rawComp = null;

    // Added to measure the execution time when the profiler setting is enabled
    protected ExecutionTimeStopWatch profilerSW;
    protected String nodeJobSignature;
    protected String taskId;
    protected String indexType;

    // For the experiment
    private static final Logger LOGGER = Logger.getLogger(IndexSearchOperatorNodePushable.class.getName());
    private static final Level LVL = Level.WARNING;

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

        // Used for LIMIT push-down
        this.limitNumberOfResult = opDesc.getLimitNumberOfResult();
        this.useOpercationCallbackProceedReturnResult = opDesc.getUseOpercationCallbackProceedReturnResult();
        if (this.useOpercationCallbackProceedReturnResult && this.limitNumberOfResult > -1) {
            this.useLimitNumberOfResult = true;
            this.rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            this.valuesForOpercationCallbackProceedReturnResult = opDesc
                    .getValuesForOpercationCallbackProceedReturnResult();
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
        //        if (useLimitNumberOfResult) {
        //            LOGGER.log(LVL, "***** [Index-only experiment] INDEX-SEARCH LIMIT push-down will be applied. LIMIT count:"
        //                    + limitNumberOfResult + " " + searchedTupleCount);
        //        }
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
                //                ByteArrayInputStream inStreamZero = new ByteArrayInputStream(tuple.getFieldData(i),
                //                        tuple.getFieldStart(i), tuple.getFieldLength(i));
                //
                //                int fieldNo = i;
                //                if (retainInput) {
                //                    fieldNo += frameTuple.getFieldCount();
                //                }
                //
                //                Object test = outputRecDesc.getFields()[fieldNo].deserialize(new DataInputStream(inStreamZero));
                //                System.out.println(test + " ");
                //
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }

            //            String testMsg = "";
            //            for (int i = 0; i < tuple.getFieldCount(); i++) {
            //                ByteArrayInputStream inStreamZero = new ByteArrayInputStream(tuple.getFieldData(i),
            //                        tuple.getFieldStart(i), tuple.getFieldLength(i));
            //                int fieldNo = i;
            //                if (retainInput) {
            //                    fieldNo += frameTuple.getFieldCount();
            //                }
            //                Object test = outputRecDesc.getFields()[fieldNo].deserialize(new DataInputStream(inStreamZero));
            //                //                System.out.print(test + " ");
            //                testMsg += test + " ";
            //            }
            //            System.out.print(testMsg + "\n");

            // Added to measure the execution time when the profiler setting is enabled
            if (!ExecutionTimeProfiler.PROFILE_MODE) {
                FrameUtils
                        .appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            } else {
                // If the profiler is ON, then we use stopwatch to measure the time.
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize(), profilerSW);
            }

            if (useLimitNumberOfResult) {
                int lastIndex = tuple.getFieldCount() - 1;

                // Only count the number of tuples that the proceedReturnResult was successful.
                // This means that we are getting a trustworthy tuple.
                if (rawComp.compare(tuple.getFieldData(lastIndex), tuple.getFieldStart(lastIndex),
                        tuple.getFieldLength(lastIndex), valuesForOpercationCallbackProceedReturnResult,
                        tuple.getFieldLength(lastIndex), tuple.getFieldLength(lastIndex)) == 0) {
                    searchedTupleCount++;
                }

                // If we hit the LIMIT number, no more tuples will be fetched.
                if (searchedTupleCount >= limitNumberOfResult) {
                    LOGGER.log(LVL,
                            "***** [Index-only experiment] INDEX-SEARCH LIMIT push-down has been applied. LIMIT count:"
                                    + limitNumberOfResult);
                    break;
                }
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

        if (!useLimitNumberOfResult || (useLimitNumberOfResult && searchedTupleCount < limitNumberOfResult)) {
            accessor.reset(buffer);
            int tupleCount = accessor.getTupleCount();
            LOGGER.log(LVL, "***** [Index-only experiment] total tuple count:" + tupleCount);
            try {
                for (int i = 0; i < tupleCount; i++) {
                    if (!useLimitNumberOfResult || (useLimitNumberOfResult && searchedTupleCount < limitNumberOfResult)) {
                        resetSearchPredicate(i);
                        cursor.reset();
                        indexAccessor.search(cursor, searchPred);
                        writeSearchResults(i);
                    }
                }
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
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
