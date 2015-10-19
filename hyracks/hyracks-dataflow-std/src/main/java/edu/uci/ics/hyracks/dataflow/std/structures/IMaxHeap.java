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

package edu.uci.ics.hyracks.dataflow.std.structures;

public interface IMaxHeap<T> extends IHeap<T> {
    /**
     * Removes and returns the largest element in the tree.
     * Make sure the heap is not empty (by {@link #isEmpty()}) before calling this method
     *
     * @param result
     */
    void getMax(T result);

    /**
     * Returns (and does NOT remove) the largest element in the tree
     *
     * @param result is the object that will eventually contain maximum entry
     *               pointer
     */
    void peekMax(T result);

    /**
     * Removes the current max and insert a new element.
     * Normally it is a faster way to call getMax() && insert() together
     *
     * @param newElement
     */
    void replaceMax(T newElement);

}
