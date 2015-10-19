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
package edu.uci.ics.hyracks.control.nc.resources.memory;

import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.hyracks.api.resources.memory.IMemoryManager;

public class MemoryManager implements IMemoryManager {
    private final long maxMemory;

    private final AtomicLong memory;

    public MemoryManager(long maxMemory) {
        this.maxMemory = maxMemory;
        this.memory = new AtomicLong(maxMemory);
    }

    @Override
    public long getMaximumMemory() {
        return maxMemory;
    }

    @Override
    public long getAvailableMemory() {
        return memory.get();
    }

    @Override
    public boolean allocate(long memory) {
        // commented as now the deallocation is not implemented yet.
        //        if (this.memory.addAndGet(-memory) < 0) {
        //            this.memory.addAndGet(memory);
        //            return false;
        //        }
        return true;
    }

    @Override
    public void deallocate(long memory) {
        this.memory.addAndGet(memory);
    }
}