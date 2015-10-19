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
package edu.uci.ics.hyracks.storage.am.lsm.common.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexInsertUpdateDeleteOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMIndexInsertUpdateDeleteOperatorNodePushable extends IndexInsertUpdateDeleteOperatorNodePushable {

    protected FrameTupleAppender appender;

    @Override
    public void open() throws HyracksDataException {
        super.open();
        appender = new FrameTupleAppender(writeBuffer);
    }

    public LSMIndexInsertUpdateDeleteOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, IndexOperation op) {
        super(opDesc, ctx, partition, fieldPermutation, recordDescProvider, op);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int nextFlushTupleIndex = 0;
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);

                switch (op) {
                    case INSERT: {
                        if (!lsmAccessor.tryInsert(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.insert(tuple);
                        }
                        break;
                    }
                    case DELETE: {
                        if (!lsmAccessor.tryDelete(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.delete(tuple);
                        }
                        break;
                    }
                    case UPSERT: {
                        if (!lsmAccessor.tryUpsert(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.upsert(tuple);
                        }
                        break;
                    }
                    case UPDATE: {
                        if (!lsmAccessor.tryUpdate(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.update(tuple);
                        }
                        break;
                    }
                    default: {
                        throw new HyracksDataException(
                                "Unsupported operation " + op + " in tree index InsertUpdateDelete operator");
                    }
                }
            } catch (HyracksDataException e) {
                throw e;
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
        if (nextFlushTupleIndex == 0) {
            // No partial flushing was necessary. Forward entire frame.
            writeBuffer.ensureFrameSize(buffer.capacity());
            FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
            FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
        } else {
            // Flush remaining partial frame.
            flushPartialFrame(nextFlushTupleIndex, tupleCount);
        }
    }

    private void flushPartialFrame(int startTupleIndex, int endTupleIndex) throws HyracksDataException {
        for (int i = startTupleIndex; i < endTupleIndex; i++) {
            FrameUtils.appendToWriter(writer, appender, accessor, i);
        }
        appender.flush(writer, true);
    }
}
