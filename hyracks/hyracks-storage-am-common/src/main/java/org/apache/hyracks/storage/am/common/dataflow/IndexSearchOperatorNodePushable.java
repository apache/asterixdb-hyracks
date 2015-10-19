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

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
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
    protected final boolean retainInput;
    protected FrameTupleReference frameTuple;

    protected final boolean retainNull;
    protected ArrayTupleBuilder nullTupleBuild;
    protected INullWriter nullWriter;

    protected final int[] minFilterFieldIndexes;
    protected final int[] maxFilterFieldIndexes;
    protected PermutingFrameTupleReference minFilterKey;
    protected PermutingFrameTupleReference maxFilterKey;

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
        return indexAccessor.createSearchCursor(false);
    }

    protected abstract int getFieldCount();

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        writer.open();
        indexHelper.open();
        index = indexHelper.getIndexInstance();

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
            searchPred = createSearchPredicate();
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
        while (cursor.hasNext()) {
            matched = true;
            tb.reset();
            cursor.next();
            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                    tb.getSize());
        }

        if (!matched && retainInput && retainNull) {
            FrameUtils.appendConcatToWriter(writer, appender, accessor, tupleIndex,
                    nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0, nullTupleBuild.getSize());
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
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
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
