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
package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class ExternalBTreeWithBuddyOpContext implements ILSMIndexOperationContext {
    private IndexOperation op;
    private MultiComparator bTreeCmp;
    private MultiComparator buddyBTreeCmp;
    public final List<ILSMComponent> componentHolder;
    private final List<ILSMComponent> componentsToBeMerged;
    private final List<ILSMComponent> componentsToBeReplicated;
    public final ISearchOperationCallback searchCallback;
    private final int targetIndexVersion;
    public ISearchPredicate searchPredicate;

    public ExternalBTreeWithBuddyOpContext(IBinaryComparatorFactory[] btreeCmpFactories,
            IBinaryComparatorFactory[] buddyBtreeCmpFactories, ISearchOperationCallback searchCallback,
            int targetIndexVersion) {

        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.componentsToBeReplicated = new LinkedList<ILSMComponent>();
        this.searchCallback = searchCallback;
        this.targetIndexVersion = targetIndexVersion;
        this.bTreeCmp = MultiComparator.create(btreeCmpFactories);
        this.buddyBTreeCmp = MultiComparator.create(buddyBtreeCmpFactories);
    }

    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing. this should never be called for disk only indexes
    }

    @Override
    public void reset() {
        componentHolder.clear();
        componentsToBeMerged.clear();
        componentsToBeReplicated.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public MultiComparator getBTreeMultiComparator() {
        return bTreeCmp;
    }

    public MultiComparator getBuddyBTreeMultiComparator() {
        return buddyBTreeCmp;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    // This should never be needed for disk only indexes
    @Override
    public IModificationOperationCallback getModificationCallback() {
        return null;
    }

    @Override
    public List<ILSMComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    public int getTargetIndexVersion() {
        return targetIndexVersion;
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        this.searchPredicate = searchPredicate;
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        return searchPredicate;
    }

    @Override
    public List<ILSMComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }

    @Override
    public void setUseOperationCallbackProceedReturnResult(boolean useOperationCallbackProceedReturnResult) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean getUseOperationCallbackProceedReturnResult() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setRecordDescForProceedReturnResult(RecordDescriptor rDescForProceedReturnResult) {
        // TODO Auto-generated method stub

    }

    @Override
    public RecordDescriptor getRecordDescForProceedReturnResult() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setValuesForProceedReturnResult(byte[] valuesForProceedReturnResult) {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] getValuesForProceedReturnResult() {
        // TODO Auto-generated method stub
        return null;
    }
}