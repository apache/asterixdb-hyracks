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
package edu.uci.ics.hyracks.dataflow.common.io;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class RunFileReader implements IFrameReader {
    private final FileReference file;
    private final IIOManager ioManager;
    private final long size;

    private IFileHandle handle;
    private long readPtr;

    public RunFileReader(FileReference file, IIOManager ioManager, long size) {
        this.file = file;
        this.ioManager = ioManager;
        this.size = size;
    }

    @Override
    public void open() throws HyracksDataException {
        handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY, null);
        readPtr = 0;
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        if (readPtr >= size) {
            return false;
        }
        frame.reset();
        int readLength = ioManager.syncRead(handle, readPtr, frame.getBuffer());
        if (readLength <= 0) {
            throw new HyracksDataException("Premature end of file");
        }
        readPtr += readLength;
        frame.ensureFrameSize(frame.getMinSize() * FrameHelper.deserializeNumOfMinFrame(frame.getBuffer()));
        if (frame.getBuffer().hasRemaining()) {
            if (readPtr < size) {
                readLength = ioManager.syncRead(handle, readPtr, frame.getBuffer());
                if (readLength < 0) {
                    throw new HyracksDataException("Premature end of file");
                }
                readPtr += readLength;
            }
            if (frame.getBuffer().hasRemaining()) { // file is vanished.
                FrameHelper.clearRemainingFrame(frame.getBuffer(), frame.getBuffer().position());
            }
        }
        frame.getBuffer().flip();
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        ioManager.close(handle);
    }

    public long getFileSize() {
        return size;
    }
}