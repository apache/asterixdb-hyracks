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
package org.apache.hyracks.control.nc.io;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOFuture;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;

public class IOManager implements IIOManager {
    private static final String WORKSPACE_FILE_SUFFIX = ".waf";
    private final List<IODeviceHandle> ioDevices;
    private Executor executor;
    private final List<IODeviceHandle> workAreaIODevices;
    private int workAreaDeviceIndex;

    public IOManager(List<IODeviceHandle> devices, Executor executor) throws HyracksException {
        this(devices);
        this.executor = executor;
    }

    public IOManager(List<IODeviceHandle> devices) throws HyracksException {
        this.ioDevices = Collections.unmodifiableList(devices);
        workAreaIODevices = new ArrayList<IODeviceHandle>();
        for (IODeviceHandle d : ioDevices) {
            if (d.getWorkAreaPath() != null) {
                new File(d.getPath(), d.getWorkAreaPath()).mkdirs();
                workAreaIODevices.add(d);
            }
        }
        if (workAreaIODevices.isEmpty()) {
            throw new HyracksException("No devices with work areas found");
        }
        workAreaDeviceIndex = 0;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public List<IODeviceHandle> getIODevices() {
        return ioDevices;
    }

    @Override
    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        FileHandle fHandle = new FileHandle(fileRef);
        try {
            fHandle.open(rwMode, syncMode);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return fHandle;
    }

    @Override
    public int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        try {
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((FileHandle) fHandle).getFileChannel().write(data, offset);
                if (len < 0) {
                    throw new HyracksDataException("Error writing to file: "
                            + ((FileHandle) fHandle).getFileReference().toString());
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * Please do check the return value of this read!
     *
     * @param fHandle
     * @param offset
     * @param data
     * @return The number of bytes read, possibly zero, or -1 if the given offset is greater than or equal to the file's current size
     * @throws HyracksDataException
     */
    @Override
    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        try {
            int n = 0;
            int remaining = data.remaining();
            while (remaining > 0) {
                int len = ((FileHandle) fHandle).getFileChannel().read(data, offset);
                if (len < 0) {
                    return n == 0 ? -1 : n;
                }
                remaining -= len;
                offset += len;
                n += len;
            }
            return n;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public IIOFuture asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncWriteRequest req = new AsyncWriteRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public IIOFuture asyncRead(IFileHandle fHandle, long offset, ByteBuffer data) {
        AsyncReadRequest req = new AsyncReadRequest((FileHandle) fHandle, offset, data);
        executor.execute(req);
        return req;
    }

    @Override
    public void close(IFileHandle fHandle) throws HyracksDataException {
        try {
            ((FileHandle) fHandle).close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        IODeviceHandle dev = workAreaIODevices.get(workAreaDeviceIndex);
        workAreaDeviceIndex = (workAreaDeviceIndex + 1) % workAreaIODevices.size();
        String waPath = dev.getWorkAreaPath();
        File waf;
        try {
            waf = File.createTempFile(prefix, WORKSPACE_FILE_SUFFIX, new File(dev.getPath(), waPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return dev.createFileReference(waPath + File.separator + waf.getName());
    }

    private abstract class AsyncRequest implements IIOFuture, Runnable {
        protected final FileHandle fHandle;
        protected final long offset;
        protected final ByteBuffer data;
        private boolean complete;
        private HyracksDataException exception;
        private int result;

        private AsyncRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            this.fHandle = fHandle;
            this.offset = offset;
            this.data = data;
            complete = false;
            exception = null;
        }

        @Override
        public void run() {
            HyracksDataException hde = null;
            int res = -1;
            try {
                res = performOperation();
            } catch (HyracksDataException e) {
                hde = e;
            }
            synchronized (this) {
                exception = hde;
                result = res;
                complete = true;
                notifyAll();
            }
        }

        protected abstract int performOperation() throws HyracksDataException;

        @Override
        public synchronized int synchronize() throws HyracksDataException, InterruptedException {
            while (!complete) {
                wait();
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        }

        @Override
        public synchronized boolean isComplete() {
            return complete;
        }
    }

    private class AsyncReadRequest extends AsyncRequest {
        private AsyncReadRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncRead(fHandle, offset, data);
        }
    }

    private class AsyncWriteRequest extends AsyncRequest {
        private AsyncWriteRequest(FileHandle fHandle, long offset, ByteBuffer data) {
            super(fHandle, offset, data);
        }

        @Override
        protected int performOperation() throws HyracksDataException {
            return syncWrite(fHandle, offset, data);
        }
    }

    @Override
    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException {
        try {
            ((FileHandle) fileHandle).sync(metadata);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void deleteWorkspaceFiles() {
        for (IODeviceHandle ioDevice : workAreaIODevices) {
            File workspaceFolder = new File(ioDevice.getPath(), ioDevice.getWorkAreaPath());
            if (workspaceFolder.exists() && workspaceFolder.isDirectory()) {
                File[] workspaceFiles = workspaceFolder.listFiles(WORKSPACE_FILES_FILTER);
                for (File workspaceFile : workspaceFiles) {
                    workspaceFile.delete();
                }
            }
        }
    }

    private static final FilenameFilter WORKSPACE_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return name.endsWith(WORKSPACE_FILE_SUFFIX);
        }
    };
}