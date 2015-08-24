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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;

public class LSMInvertedIndexSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    protected final IInvertedIndexSearchModifier searchModifier;
    protected final int queryFieldIndex;
    protected final int invListFields;

    public LSMInvertedIndexSearchOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int queryFieldIndex,
            IInvertedIndexSearchModifier searchModifier, int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) {
        super(opDesc, ctx, partition, recordDescProvider, minFilterFieldIndexes, maxFilterFieldIndexes);
        this.searchModifier = searchModifier;
        this.queryFieldIndex = queryFieldIndex;
        // If retainInput is true, the frameTuple is created in IndexSearchOperatorNodePushable.open().
        if (!opDesc.getRetainInput()) {
            this.frameTuple = new FrameTupleReference();
        }
        AbstractLSMInvertedIndexOperatorDescriptor invIndexOpDesc = (AbstractLSMInvertedIndexOperatorDescriptor) opDesc;
        invListFields = invIndexOpDesc.getInvListsTypeTraits().length;
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        AbstractLSMInvertedIndexOperatorDescriptor invIndexOpDesc = (AbstractLSMInvertedIndexOperatorDescriptor) opDesc;
        return new InvertedIndexSearchPredicate(invIndexOpDesc.getTokenizerFactory().createTokenizer(), searchModifier,
                minFilterKey, maxFilterKey);
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        frameTuple.reset(accessor, tupleIndex);
        InvertedIndexSearchPredicate invIndexSearchPred = (InvertedIndexSearchPredicate) searchPred;
        invIndexSearchPred.setQueryTuple(frameTuple);
        invIndexSearchPred.setQueryFieldIndex(queryFieldIndex);
        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected int getFieldCount() {
        return invListFields;
    }
}
