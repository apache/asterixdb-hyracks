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
package edu.uci.ics.hyracks.algebricks.runtime.operators.base;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameAppender;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAppender;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFramePushRuntime extends AbstractOneInputOneOutputPushRuntime {

    protected IFrameAppender appender;
    protected IFrame frame;
    protected FrameTupleAccessor tAccess;
    protected FrameTupleReference tRef;

    protected final void initAccessAppend(IHyracksTaskContext ctx) throws HyracksDataException {
        frame = new VSizeFrame(ctx);
        appender = new FrameTupleAppender(frame);
        tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    protected final void initAccessAppendRef(IHyracksTaskContext ctx) throws HyracksDataException {
        initAccessAppend(ctx);
        tRef = new FrameTupleReference();
    }

    @Override
    public void close() throws HyracksDataException {
        flushIfNotFailed();
        writer.close();
    }

    protected void flushAndReset() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            appender.flush(writer, true);
        }
    }

    protected void flushIfNotFailed() throws HyracksDataException {
        if (!failed) {
            flushAndReset();
        }
    }

    protected IFrameTupleAppender getTupleAppender() {
        return (FrameTupleAppender) appender;
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb) throws HyracksDataException {
        appendToFrameFromTupleBuilder(tb, false);
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb, boolean flushFrame) throws HyracksDataException {
        FrameUtils.appendToWriter(writer, getTupleAppender(), tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                tb.getSize());
        if (flushFrame) {
            flushAndReset();
        }
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList) throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, false);
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList, boolean flushFrame)
            throws HyracksDataException {
        FrameUtils.appendProjectionToWriter(writer, getTupleAppender(), tAccess, tIndex, projectionList);
        if (flushFrame) {
            flushAndReset();
        }
    }

    protected void appendTupleToFrame(int tIndex) throws HyracksDataException {
        FrameUtils.appendToWriter(writer, getTupleAppender(), tAccess, tIndex);
    }

    protected void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, getTupleAppender(), accessor0, tIndex0, accessor1, tIndex1);
    }
}
