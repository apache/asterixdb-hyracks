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

package edu.uci.ics.hyracks.dataflow.std.sort.buffermanager;

public interface IFrameFreeSlotPolicy {

    /**
     * Find the best fit frame id which can hold the data, and then pop it out from the index.
     * Return -1 is failed to find any.
     *
     * @param tobeInsertedSize the actual size of the data which should include
     *                         the meta data like the field offset and the tuple
     *                         count extra size
     * @return the best fit frame id
     */
    int popBestFit(int tobeInsertedSize);

    /**
     * Register the new free slot into the index
     *
     * @param frameID
     * @param freeSpace
     */
    void pushNewFrame(int frameID, int freeSpace);

    /**
     * Clear all the existing free slot information.
     */
    void reset();

}
