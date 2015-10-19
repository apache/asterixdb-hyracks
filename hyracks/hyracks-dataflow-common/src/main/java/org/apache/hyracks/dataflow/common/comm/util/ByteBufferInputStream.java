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

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {

    private ByteBuffer buffer;

    private int position;

    public ByteBufferInputStream() {
    }

    public void setByteBuffer(ByteBuffer buffer, int position) {
        this.buffer = buffer;
        this.position = position;
    }

    @Override
    public int read() {
        int remaining = buffer.capacity() - position;
        int value = remaining > 0 ? (buffer.array()[position++] & 0xff) : -1;
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        int remaining = buffer.capacity() - position;
        if (remaining == 0) {
            return -1;
        }
        int l = Math.min(length, remaining);
        System.arraycopy(buffer.array(), position, bytes, offset, l);
        position += l;
        return l;
    }
}