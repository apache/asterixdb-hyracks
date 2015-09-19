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
package org.apache.hyracks.storage.common.file;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public class TransientFileMapManager implements IFileMapManager {
    private static final long serialVersionUID = 1L;

    private Map<Integer, FileReference> id2nameMap = new HashMap<Integer, FileReference>();
    private Map<FileReference, Integer> name2IdMap = new HashMap<FileReference, Integer>();
    private int idCounter = 0;

    @Override
    public FileReference lookupFileName(int fileId) throws HyracksDataException {
        FileReference fRef = id2nameMap.get(fileId);
        if (fRef == null) {
            throw new HyracksDataException("No mapping found for id: " + fileId);
        }
        return fRef;
    }

    @Override
    public int lookupFileId(FileReference fileRef) throws HyracksDataException {
        Integer fileId = name2IdMap.get(fileRef);
        if (fileId == null) {
            throw new HyracksDataException("No mapping found for name: " + fileRef);
        }
        return fileId;
    }

    @Override
    public boolean isMapped(FileReference fileRef) {
        return name2IdMap.containsKey(fileRef);
    }

    @Override
    public boolean isMapped(int fileId) {
        return id2nameMap.containsKey(fileId);
    }

    @Override
    public void unregisterFile(int fileId) throws HyracksDataException {
        FileReference fileRef = id2nameMap.remove(fileId);
        name2IdMap.remove(fileRef);
    }

    @Override
    public void registerFile(FileReference fileRef) throws HyracksDataException {
        Integer fileId = idCounter++;
        id2nameMap.put(fileId, fileRef);
        name2IdMap.put(fileRef, fileId);
    }

    @Override
    public int registerMemoryFile() {
        Integer fileId = idCounter++;
        id2nameMap.put(fileId, null);
        return fileId;
    }

    @Override
    public void unregisterMemFile(int fileId) throws HyracksDataException {
        id2nameMap.remove(fileId);
    }
}