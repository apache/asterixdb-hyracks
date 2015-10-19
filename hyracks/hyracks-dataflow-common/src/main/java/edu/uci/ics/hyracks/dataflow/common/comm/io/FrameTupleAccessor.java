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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.util.IntSerDeUtils;

/**
 * FrameTupleCursor is used to navigate over tuples in a Frame. A frame is
 * formatted with tuple data concatenated starting at offset 1, one tuple after
 * another. The first byte is used to notify how big the frame is, so the maximum frame size is 255 * initialFrameSetting.
 * Offset FS - 4 holds an int indicating the number of tuples (N) in
 * the frame. FS - ((i + 1) * 4) for i from 0 to N - 1 holds an int indicating
 * the offset of the (i + 1)^th tuple. Every tuple is organized as a sequence of
 * ints indicating the end of each field in the tuple relative to the end of the
 * field slots.
 */
public class FrameTupleAccessor implements IFrameTupleAccessor {
    private int tupleCountOffset;
    private final RecordDescriptor recordDescriptor;
    private ByteBuffer buffer;
    private int start;

    public FrameTupleAccessor(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        reset(buffer, 0, buffer.limit());
    }

    public void reset(ByteBuffer buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.tupleCountOffset = start + FrameHelper.getTupleCountOffset(length);
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getTupleCount() {
        return IntSerDeUtils.getInt(buffer.array(), tupleCountOffset);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        int offset = tupleIndex == 0 ?
                FrameConstants.TUPLE_START_OFFSET :
                IntSerDeUtils.getInt(buffer.array(), tupleCountOffset - 4 * tupleIndex);
        return start + offset;
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return start + IntSerDeUtils
                .getInt(buffer.array(), tupleCountOffset - FrameConstants.SIZE_LEN * (tupleIndex + 1));
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ?
                0 :
                IntSerDeUtils
                        .getInt(buffer.array(), getTupleStartOffset(tupleIndex) + (fIdx - 1) * FrameConstants.SIZE_LEN);
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return IntSerDeUtils.getInt(buffer.array(), getTupleStartOffset(tupleIndex) + fIdx * FrameConstants.SIZE_LEN);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getFieldSlotsLength() {
        return getFieldCount() * FrameConstants.SIZE_LEN;
    }

    public void prettyPrint(String prefix) {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream dis = new DataInputStream(bbis);
        int tc = getTupleCount();
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TC: " + tc).append("\n");
        for (int i = 0; i < tc; ++i) {
            prettyPrint(i, bbis, dis, sb);
        }
        System.err.println(sb.toString());
    }

    public void prettyPrint() {
        prettyPrint("");
    }

    protected void prettyPrint(int tid, ByteBufferInputStream bbis, DataInputStream dis, StringBuilder sb) {
        sb.append(" tid" + tid + ":(" + getTupleStartOffset(tid) + ", " + getTupleEndOffset(tid) + ")[");
        for (int j = 0; j < getFieldCount(); ++j) {
            sb.append("f" + j + ":(" + getFieldStartOffset(tid, j) + ", " + getFieldEndOffset(tid, j) + ") ");
            sb.append("{");
            bbis.setByteBuffer(buffer, getTupleStartOffset(tid) + getFieldSlotsLength() + getFieldStartOffset(tid, j));
            try {
                sb.append(recordDescriptor.getFields()[j].deserialize(dis));
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            sb.append("}");
        }
        sb.append("\n");
    }

    public void prettyPrint(int tid) {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream dis = new DataInputStream(bbis);
        StringBuilder sb = new StringBuilder();
        prettyPrint(tid, bbis, dis, sb);
        System.err.println(sb.toString());
    }

    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }
}
