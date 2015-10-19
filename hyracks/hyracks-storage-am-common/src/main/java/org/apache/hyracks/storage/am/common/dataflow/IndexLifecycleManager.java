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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

public class IndexLifecycleManager implements IIndexLifecycleManager, ILifeCycleComponent {
    private static final long DEFAULT_MEMORY_BUDGET = 1024 * 1024 * 100; // 100 megabytes

    private final Map<Long, IndexInfo> indexInfos;
    private final long memoryBudget;
    private final ILocalResourceRepository localResourcesRepository;
    private long memoryUsed;

    public IndexLifecycleManager(ILocalResourceRepository localResourcesRepository) {
        this(localResourcesRepository, DEFAULT_MEMORY_BUDGET);
    }

    public IndexLifecycleManager(ILocalResourceRepository localResourcesRepository, long memoryBudget) {
        this.localResourcesRepository = localResourcesRepository;
        this.indexInfos = new HashMap<Long, IndexInfo>();
        this.memoryBudget = memoryBudget;
        this.memoryUsed = 0;
    }

    private boolean evictCandidateIndex() throws HyracksDataException {
        // Why min()? As a heuristic for eviction, we will take an open index
        // (an index consuming memory)
        // that is not being used (refcount == 0) and has been least recently
        // used. The sort order defined
        // for IndexInfo maintains this. See IndexInfo.compareTo().
        IndexInfo info = Collections.min(indexInfos.values());
        if (info.referenceCount != 0 || !info.isOpen) {
            return false;
        }

        info.index.deactivate();
        memoryUsed -= info.index.getMemoryAllocationSize();
        info.isOpen = false;

        return true;
    }

    private class IndexInfo implements Comparable<IndexInfo> {
        private final IIndex index;
        private int referenceCount;
        private long lastAccess;
        private boolean isOpen;

        public IndexInfo(IIndex index) {
            this.index = index;
            this.lastAccess = -1;
            this.referenceCount = 0;
            this.isOpen = false;
        }

        public void touch() {
            lastAccess = System.currentTimeMillis();
            referenceCount++;
        }

        public void untouch() {
            lastAccess = System.currentTimeMillis();
            referenceCount--;
        }

        @Override
        public int compareTo(IndexInfo i) {
            // sort by (isOpen, referenceCount, lastAccess) ascending, where
            // true < false
            //
            // Example sort order:
            // -------------------
            // (F, 0, 70) <-- largest
            // (F, 0, 60)
            // (T, 10, 80)
            // (T, 10, 70)
            // (T, 9, 90)
            // (T, 0, 100) <-- smallest
            if (isOpen && !i.isOpen) {
                return -1;
            } else if (!isOpen && i.isOpen) {
                return 1;
            } else {
                if (referenceCount < i.referenceCount) {
                    return -1;
                } else if (referenceCount > i.referenceCount) {
                    return 1;
                } else {
                    if (lastAccess < i.lastAccess) {
                        return -1;
                    } else if (lastAccess > i.lastAccess) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }

        }

        public String toString() {
            return "{index: " + index + ", isOpen: " + isOpen + ", refCount: " + referenceCount + ", lastAccess: "
                    + lastAccess + "}";
        }
    }

    @Override
    public List<IIndex> getOpenIndexes() {
        List<IIndex> openIndexes = new ArrayList<IIndex>();
        for (IndexInfo i : indexInfos.values()) {
            if (i.isOpen) {
                openIndexes.add(i.index);
            }
        }
        return openIndexes;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(boolean dumpState, OutputStream outputStream) throws IOException {
        if (dumpState) {
            dumpState(outputStream);
        }

        for (IndexInfo i : indexInfos.values()) {
            if (i.isOpen) {
                i.index.deactivate();
            }
        }
    }

    public void dumpState(OutputStream os) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("Memory budget = %d\n", memoryBudget));
        sb.append(String.format("Memory used = %d\n", memoryUsed));

        String headerFormat = "%-20s %-10s %-20s %-20s %-20s\n";
        String rowFormat = "%-20d %-10b %-20d %-20s %-20s\n";
        sb.append(String.format(headerFormat, "ResourceID", "Open", "Reference Count", "Last Access", "Index Name"));
        IndexInfo ii;
        for (Map.Entry<Long, IndexInfo> entry : indexInfos.entrySet()) {
            ii = entry.getValue();
            sb.append(String.format(rowFormat, entry.getKey(), ii.isOpen, ii.referenceCount, ii.lastAccess, ii.index));
        }
        os.write(sb.toString().getBytes());
    }

    @Override
    public void register(String resourceName, IIndex index) throws HyracksDataException {
        long resourceID = getResourceID(resourceName);
        if (indexInfos.containsKey(resourceID)) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " already exists.");
        }

        indexInfos.put(resourceID, new IndexInfo(index));
    }

    @Override
    public void open(String resourceName) throws HyracksDataException {
        long resourceID = getResourceID(resourceName);
        IndexInfo info = indexInfos.get(resourceID);
        if (info == null) {
            throw new HyracksDataException("Failed to open index with resource ID " + resourceID
                    + " since it does not exist.");
        }

        if (!info.isOpen) {
            long inMemorySize = info.index.getMemoryAllocationSize();
            while (memoryUsed + inMemorySize > memoryBudget) {
                if (!evictCandidateIndex()) {
                    throw new HyracksDataException("Cannot activate index since memory budget would be exceeded.");
                }
            }
            info.index.activate();
            info.isOpen = true;
            memoryUsed += inMemorySize;
        }
        info.touch();
    }

    @Override
    public void close(String resourceName) throws HyracksDataException {
        long resourceID = getResourceID(resourceName);
        indexInfos.get(resourceID).untouch();
    }

    @Override
    public IIndex getIndex(String resourceName) throws HyracksDataException {
        long resourceID = getResourceID(resourceName);
        IndexInfo info = indexInfos.get(resourceID);
        return info == null ? null : info.index;
    }

    @Override
    public void unregister(String resourceName) throws HyracksDataException {
        long resourceID = getResourceID(resourceName);
        IndexInfo info = indexInfos.remove(resourceID);
        if (info == null) {
            throw new HyracksDataException("Index with resource ID " + resourceID + " does not exist.");
        }

        if (info.referenceCount != 0) {
            indexInfos.put(resourceID, info);
            throw new HyracksDataException("Cannot remove index while it is open.");
        }

        if (info.isOpen) {
            info.index.deactivate();
            memoryUsed -= info.index.getMemoryAllocationSize();
        }
    }

    private long getResourceID(String resourceName) throws HyracksDataException {
        LocalResource lr = localResourcesRepository.getResourceByName(resourceName);
        return lr == null ? -1 : lr.getResourceId();
    }

    @Override
    public IIndex getIndex(int datasetID, long resourceID) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }
}