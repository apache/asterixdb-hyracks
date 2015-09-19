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

public class TuplePointer implements IResetable<TuplePointer> {
    public static final int INVALID_ID = -1;
    public int frameIndex;
    public int tupleIndex;

    public TuplePointer() {
        this(INVALID_ID, INVALID_ID);
    }

    public TuplePointer(int frameId, int tupleId) {
        reset(frameId, tupleId);
    }

    public void reset(TuplePointer other) {
        reset(other.frameIndex, other.tupleIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TuplePointer that = (TuplePointer) o;

        if (frameIndex != that.frameIndex)
            return false;
        return tupleIndex == that.tupleIndex;

    }

    @Override
    public int hashCode() {
        int result = frameIndex;
        result = 31 * result + tupleIndex;
        return result;
    }

    public void reset(int frameId, int tupleId) {
        this.frameIndex = frameId;
        this.tupleIndex = tupleId;
    }

}