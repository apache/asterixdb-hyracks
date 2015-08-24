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
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;
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
        flushAndReset(null);
        //        if (appender.getTupleCount() > 0) {
        //            appender.flush(writer, true);
        //        }
    }

    // the same as the flushAndReset() in the above. Added StopWatch to measure the execution time
    protected void flushAndReset(ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
                appender.flush(writer, true);
            } else {
                execTimeProfilerSW.suspend();
                appender.flush(writer, true);
                execTimeProfilerSW.resume();
            }
        }
    }

    protected void flushIfNotFailed() throws HyracksDataException {
        if (!failed) {
            flushAndReset(null);
        }
    }

    // the same as the above.
    protected void flushIfNotFailed(ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        if (!failed) {
            if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
                flushAndReset(null);
            } else {
                flushAndReset(execTimeProfilerSW);
            }
        }
    }

    protected IFrameTupleAppender getTupleAppender() {
        return (FrameTupleAppender) appender;
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb) throws HyracksDataException {
        appendToFrameFromTupleBuilder(tb, false, null);
    }

    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb, boolean flushFrame) throws HyracksDataException {
        appendToFrameFromTupleBuilder(tb, flushFrame, null);
    }

    // the same as the appendToFrameFromTupleBuilder() in the above. Added StopWatch to measure the execution time
    protected void appendToFrameFromTupleBuilder(ArrayTupleBuilder tb, boolean flushFrame,
            ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
            FrameUtils.appendToWriter(writer, getTupleAppender(), tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                    tb.getSize());
            if (flushFrame) {
                flushAndReset(null);
            }
        } else {
            FrameUtils.appendToWriter(writer, getTupleAppender(), tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                    tb.getSize(), execTimeProfilerSW);
            if (flushFrame) {
                flushAndReset(execTimeProfilerSW);
            }
        }
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList) throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, false, null);
    }

    // the same as the appendProjectionToFrame() in the above. Added StopWatch to measure the execution time
    protected void appendProjectionToFrame(int tIndex, int[] projectionList, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, false, execTimeProfilerSW);
    }

    protected void appendProjectionToFrame(int tIndex, int[] projectionList, boolean flushFrame)
            throws HyracksDataException {
        appendProjectionToFrame(tIndex, projectionList, flushFrame, null);
    }

    // the same as the appendProjectionToFrame() in the above. Added StopWatch to measure the execution time
    protected void appendProjectionToFrame(int tIndex, int[] projectionList, boolean flushFrame,
            ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
            FrameUtils.appendProjectionToWriter(writer, getTupleAppender(), tAccess, tIndex, projectionList, null);
            if (flushFrame) {
                flushAndReset(null);
            }
        } else {
            FrameUtils.appendProjectionToWriter(writer, getTupleAppender(), tAccess, tIndex, projectionList,
                    execTimeProfilerSW);
            if (flushFrame) {
                flushAndReset(execTimeProfilerSW);
            }
        }
    }

    protected void appendTupleToFrame(int tIndex) throws HyracksDataException {
        appendTupleToFrame(tIndex, null);
    }

    // the same as the appendTupleToFrame() in the above. Added StopWatch to measure the execution time
    protected void appendTupleToFrame(int tIndex, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        FrameUtils.appendToWriter(writer, getTupleAppender(), tAccess, tIndex, execTimeProfilerSW);
    }

    protected void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        appendConcat(accessor0, tIndex0, accessor1, tIndex1, null);
    }

    // the same as the appendConcat() in the above. Added StopWatch to measure the execution time
    protected void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1,
            ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, getTupleAppender(), accessor0, tIndex0, accessor1, tIndex1,
                execTimeProfilerSW);
    }
}
