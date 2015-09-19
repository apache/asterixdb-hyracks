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

package org.apache.hyracks.storage.am.lsm.rtree.multithread;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.TestOperationSelector;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.rtree.impls.AbstractLSMRTree;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeWithAntiMatterTuples.LSMRTreeWithAntiMatterTuplesAccessor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;

public class LSMRTreeWithAntiMatterTuplesTestWorker extends AbstractLSMRTreeTestWorker {

    public LSMRTreeWithAntiMatterTuplesTestWorker(DataGenThread dataGen, TestOperationSelector opSelector,
            IIndex index, int numBatches) throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException, IndexException {
        LSMRTreeWithAntiMatterTuplesAccessor accessor = (LSMRTreeWithAntiMatterTuplesAccessor) indexAccessor;
        ITreeIndexCursor searchCursor = accessor.createSearchCursor(false);
        MultiComparator cmp = accessor.getMultiComparator();
        SearchPredicate rangePred = new SearchPredicate(tuple, cmp);

        switch (op) {
            case INSERT:
                rearrangeTuple(tuple, cmp);
                accessor.insert(rearrangedTuple);
                break;

            case DELETE:
                rearrangeTuple(tuple, cmp);
                accessor.delete(rearrangedTuple);
                break;

            case SCAN:
                searchCursor.reset();
                rangePred.setSearchKey(null);
                accessor.search(searchCursor, rangePred);
                consumeCursorTuples(searchCursor);
                break;

            case MERGE:
                accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE,
                        ((AbstractLSMRTree) lsmRTree).getImmutableComponents());
                break;

            default:
                throw new HyracksDataException("Op " + op.toString() + " not supported.");
        }
    }
}
