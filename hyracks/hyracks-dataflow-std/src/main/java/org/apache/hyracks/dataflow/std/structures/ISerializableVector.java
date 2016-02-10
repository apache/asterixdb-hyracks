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

package org.apache.hyracks.dataflow.std.structures;


import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISerializableVector<T> {

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of the element to return
     * @param record the returned record will be to reset
     * @return false if the index is out of range
     *         (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    void get(int index, T record);

    /**
     * Appends the specified element to the end of this list.
     *
     */
    void append(T record) throws HyracksDataException;


    /**
     * Replaces the element at the specified position in this list with the
     * specified element.
     */
    void set(int index, T record);


    void clear();

    /**
     * Returns the number of elements in this list.  If this list contains
     * more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of elements in this list
     */
    int size();

    int getFrameCount();
}