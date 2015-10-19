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
package edu.uci.ics.hyracks.storage.am.common.tuples;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DualTupleReference implements ITupleReference {

    private PermutingTupleReference permutingTuple;
    private ITupleReference tuple;

    public DualTupleReference(int[] fieldPermutation) {
        permutingTuple = new PermutingTupleReference(fieldPermutation);
    }

    @Override
    public int getFieldCount() {
        return tuple.getFieldCount();
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return tuple.getFieldData(fIdx);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return tuple.getFieldStart(fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return tuple.getFieldLength(fIdx);
    }

    public void reset(ITupleReference tuple) {
        this.tuple = tuple;
        permutingTuple.reset(tuple);
    }

    public ITupleReference getPermutingTuple() {
        return permutingTuple;
    }
}