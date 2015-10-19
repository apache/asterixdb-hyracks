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

package edu.uci.ics.hyracks.dataflow.common.comm.io;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameFieldAppender;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.util.IntSerDeUtils;

public class FrameFixedFieldAppender extends AbstractFrameAppender implements IFrameFieldAppender {
    private final int fieldCount;
    private int lastFieldEndOffset;
    private int currentField;
    private int leftOverSize;
    private byte[] cachedLeftOverFields;

    public FrameFixedFieldAppender(int numberFields) {
        this.fieldCount = numberFields;
        this.lastFieldEndOffset = 0;
        this.currentField = 0;
        this.leftOverSize = 0;
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        super.reset(frame, clear);
        lastFieldEndOffset = 0;
        currentField = 0;
        leftOverSize = 0;
    }

    @Override
    public void flush(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        super.flush(outWriter, clearFrame);
        if (clearFrame) {
            if (leftOverSize > 0) {
                if (!canHoldNewTuple(0, leftOverSize)) {
                    throw new HyracksDataException(
                            "The given frame can not be extended to insert the leftover data from the last record");
                }
                System.arraycopy(cachedLeftOverFields, 0, array, tupleDataEndOffset, leftOverSize);
                leftOverSize = 0;
            }
        }
    }

    public boolean appendField(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (canHoldNewTuple(fieldCount, lastFieldEndOffset + length)) {
            int currentFieldDataStart = tupleDataEndOffset + fieldCount * 4 + lastFieldEndOffset;
            System.arraycopy(bytes, offset, array, currentFieldDataStart, length);
            lastFieldEndOffset = lastFieldEndOffset + length;
            IntSerDeUtils.putInt(array, tupleDataEndOffset + currentField * 4, lastFieldEndOffset);
            if (++currentField == fieldCount) {
                tupleDataEndOffset += fieldCount * 4 + lastFieldEndOffset;
                IntSerDeUtils
                        .putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                                tupleDataEndOffset);
                ++tupleCount;
                IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);

                //reset for the next tuple
                currentField = 0;
                lastFieldEndOffset = 0;
            }
            return true;
        } else {
            if (currentField > 0) {
                copyLeftOverData();
            }
            return false;
        }
    }

    private void copyLeftOverData() {
        leftOverSize = lastFieldEndOffset + fieldCount * 4;
        if (cachedLeftOverFields == null || cachedLeftOverFields.length < leftOverSize) {
            cachedLeftOverFields = new byte[leftOverSize];
        }
        System.arraycopy(array, tupleDataEndOffset, cachedLeftOverFields, 0, leftOverSize);
    }

    public boolean appendField(IFrameTupleAccessor fta, int tIndex, int fIndex) throws HyracksDataException {
        int startOffset = fta.getTupleStartOffset(tIndex);
        int fStartOffset = fta.getFieldStartOffset(tIndex, fIndex);
        int fLen = fta.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
        return appendField(fta.getBuffer().array(), startOffset + fta.getFieldSlotsLength() + fStartOffset, fLen);
    }

    public boolean hasLeftOverFields() {
        return currentField != 0;
    }
}
