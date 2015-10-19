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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.dataflow.value.ILinearizeComparator;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMRTreeSortedCursor extends LSMRTreeAbstractCursor {

    // TODO: This class can be removed and instead use a search cursor that uses a logic similar 
    // to the one in LSMRTreeWithAntiMatterTuplesSearchCursor
    private ILinearizeComparator linearizeCmp;
    private boolean[] depletedRtreeCursors;
    private int foundIn = -1;
    private PermutingTupleReference btreeTuple;

    public LSMRTreeSortedCursor(ILSMIndexOperationContext opCtx, ILinearizeComparatorFactory linearizer, int[] buddyBTreeFields)
            throws HyracksDataException {
        super(opCtx);
        this.linearizeCmp = linearizer.createBinaryComparator();
        this.btreeTuple = new PermutingTupleReference(buddyBTreeFields);
        reset();
    }

    public ILSMIndexOperationContext getOpCtx() {
        return opCtx;
    }

    @Override
    public void reset() throws HyracksDataException {
        depletedRtreeCursors = new boolean[numberOfTrees];
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].reset();
                try {
                    rtreeAccessors[i].search(rtreeCursors[i], rtreeSearchPredicate);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                if (rtreeCursors[i].hasNext()) {
                    rtreeCursors[i].next();
                } else {
                    depletedRtreeCursors[i] = true;
                }
            }
        } finally {
            if (open) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        while (!foundNext) {
            frameTuple = null;

            if (foundIn != -1) {
                if (rtreeCursors[foundIn].hasNext()) {
                    rtreeCursors[foundIn].next();
                } else {
                    depletedRtreeCursors[foundIn] = true;
                }
            }

            foundIn = -1;
            for (int i = 0; i < numberOfTrees; i++) {
                if (depletedRtreeCursors[i])
                    continue;

                if (frameTuple == null) {
                    frameTuple = rtreeCursors[i].getTuple();
                    foundIn = i;
                    continue;
                }

                if (linearizeCmp.compare(frameTuple.getFieldData(0), frameTuple.getFieldStart(0),
                        frameTuple.getFieldLength(0) * linearizeCmp.getDimensions(), rtreeCursors[i].getTuple()
                                .getFieldData(0), rtreeCursors[i].getTuple().getFieldStart(0), rtreeCursors[i]
                                .getTuple().getFieldLength(0) * linearizeCmp.getDimensions()) > 0) {
                    frameTuple = rtreeCursors[i].getTuple();
                    foundIn = i;
                }
            }

            if (foundIn == -1)
                return false;

            boolean killed = false;
            btreeTuple.reset(frameTuple);
            for (int i = 0; i < foundIn; i++) {
                try {
                    btreeCursors[i].reset();
                    btreeRangePredicate.setHighKey(btreeTuple, true);
                    btreeRangePredicate.setLowKey(btreeTuple, true);
                    btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                try {
                    if (btreeCursors[i].hasNext()) {
                        killed = true;
                        break;
                    }
                } finally {
                    btreeCursors[i].close();
                }
            }
            if (!killed) {
                foundNext = true;
            }
        }

        return true;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.open(initialState, searchPred);

        depletedRtreeCursors = new boolean[numberOfTrees];
        foundNext = false;
        for (int i = 0; i < numberOfTrees; i++) {
            rtreeCursors[i].reset();
            try {
                rtreeAccessors[i].search(rtreeCursors[i], rtreeSearchPredicate);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            if (rtreeCursors[i].hasNext()) {
                rtreeCursors[i].next();
            } else {
                depletedRtreeCursors[i] = true;
            }
        }
    }
}