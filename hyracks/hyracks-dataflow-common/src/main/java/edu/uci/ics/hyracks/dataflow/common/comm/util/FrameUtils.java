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
package edu.uci.ics.hyracks.dataflow.common.comm.util;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameFieldAppender;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAppender;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.ExecutionTimeProfiler;
import edu.uci.ics.hyracks.api.util.ExecutionTimeStopWatch;

public class FrameUtils {

    public static void copyWholeFrame(ByteBuffer srcFrame, ByteBuffer destFrame) {
        srcFrame.clear();
        destFrame.clear();
        destFrame.put(srcFrame);
    }

    public static void copyAndFlip(ByteBuffer srcFrame, ByteBuffer destFrame) {
        srcFrame.position(0);
        destFrame.clear();
        destFrame.put(srcFrame);
        destFrame.flip();
    }

    public static void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        writer.nextFrame(buffer);
        buffer.clear();
    }

    /**
     * A util function to append the data to appender. If the appender buffer is full, it will directly flush
     * to the given writer, which saves the detecting logic in the caller.
     * It will return the bytes that have been flushed.
     *
     * @param writer
     * @param frameTupleAppender
     * @param fieldSlots
     * @param bytes
     * @param offset
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendSkipEmptyFieldToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException {
        return appendSkipEmptyFieldToWriter(writer, frameTupleAppender, fieldSlots, bytes, offset, length, null);
    }

    // Same as the appendSkipEmptyFieldToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendSkipEmptyFieldToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            int[] fieldSlots, byte[] bytes, int offset, int length, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();

            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * A util function to append the data to appender. If the appender buffer is full, it will directly flush
     * to the given writer, which saves the detecting logic in the caller.
     * It will return the bytes that have been flushed.
     *
     * @param writer
     * @param frameTupleAppender
     * @param bytes
     * @param offset
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender, byte[] bytes,
            int offset, int length) throws HyracksDataException {
        return appendToWriter(writer, frameTupleAppender, bytes, offset, length, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender, byte[] bytes,
            int offset, int length, ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(bytes, offset, length)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();

            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.append(bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param tupleAccessor
     * @param tStartOffset
     * @param tEndOffset
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) throws HyracksDataException {
        return appendToWriter(writer, frameTupleAppender, tupleAccessor, tStartOffset, tEndOffset, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param tupleAccessor
     * @param tIndex
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        return appendToWriter(writer, frameTupleAppender, tupleAccessor, tIndex, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tIndex, ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param tupleAppender
     * @param fieldEndOffsets
     * @param byteArray
     * @param start
     * @param size
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender tupleAppender, int[] fieldEndOffsets,
            byte[] byteArray, int start, int size) throws HyracksDataException {
        return appendToWriter(writer, tupleAppender, fieldEndOffsets, byteArray, start, size, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender tupleAppender, int[] fieldEndOffsets,
            byte[] byteArray, int start, int size, ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {

            flushedBytes = tupleAppender.getBuffer().capacity();

            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            tupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }

            if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor0
     * @param tIndex0
     * @param accessor1
     * @param tIndex1
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        return appendConcatToWriter(writer, frameTupleAppender, accessor0, tIndex0, accessor1, tIndex1, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1,
            ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor0
     * @param tIndex0
     * @param fieldSlots1
     * @param bytes1
     * @param offset1
     * @param dataLen1
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1, int dataLen1)
            throws HyracksDataException {
        return appendConcatToWriter(writer, frameTupleAppender, accessor0, tIndex0, fieldSlots1, bytes1, offset1,
                dataLen1, null);
    }

    // Same as the appendToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1, int dataLen1,
            ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor
     * @param tIndex
     * @param fields
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendProjectionToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor, int tIndex, int[] fields) throws HyracksDataException {
        return appendProjectionToWriter(writer, frameTupleAppender, accessor, tIndex, fields, null);
    }

    // Same as the appendProjectionToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendProjectionToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor, int tIndex, int[] fields, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            frameTupleAppender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param appender
     * @param array
     * @param start
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender, byte[] array, int start,
            int length) throws HyracksDataException {
        return appendFieldToWriter(writer, appender, array, start, length, null);
    }

    // Same as the appendFieldToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender, byte[] array, int start,
            int length, ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!appender.appendField(array, start, length)) {
            flushedBytes = appender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            appender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!appender.appendField(array, start, length)) {
                throw new HyracksDataException("Could not write frame: the size of the tuple is too long");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param appender
     * @param accessor
     * @param tid
     * @param fid
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender,
            IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        return appendFieldToWriter(writer, appender, accessor, tid, fid, null);
    }

    // Same as the appendFieldToWriter() in the above. Added Stopwatch to measure the execution time
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender,
            IFrameTupleAccessor accessor, int tid, int fid, ExecutionTimeStopWatch execTimeProfilerSW) throws HyracksDataException {
        int flushedBytes = 0;
        if (!appender.appendField(accessor, tid, fid)) {
            flushedBytes = appender.getBuffer().capacity();
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.suspend();
                }
            }
            appender.flush(writer, true);
            // Added to measure the execution time when the profiler setting is enabled
            if (ExecutionTimeProfiler.PROFILE_MODE) {
                if (execTimeProfilerSW != null) {
                    execTimeProfilerSW.resume();
                }
            }
            if (!appender.appendField(accessor, tid, fid)) {
                throw new HyracksDataException("Could not write frame: the size of the tuple is too long");
            }
        }
        return flushedBytes;
    }

}