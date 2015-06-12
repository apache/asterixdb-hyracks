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

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;

    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private IIndexAccessor[] btreeAccessors;
    private ArrayTupleBuilder tupleBuilder;
    private ArrayTupleBuilder tupleBuilderForProceedResult;
    private boolean canCallProceedMethod = true;
    private boolean useProceedResult = false;
    private RecordDescriptor rDescForProceedReturnResult = null;
    private boolean resultOfsearchCallBackProceed = false;
    private byte[] returnValuesArrayForProccedResult = new byte[10];
    protected int proceedFailCount = 0;
    protected int proceedSuccessCount = 0;

    // For the experiment
    private static final Logger LOGGER = Logger.getLogger(LSMBTreeRangeSearchCursor.class.getName());
    private static final Level LVL = Level.WARNING;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false);
    }

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        super(opCtx, returnDeletedTuples);
        this.copyTuple = new ArrayTupleReference();
        this.reusablePred = new RangePredicate(null, null, true, true, null, null);
        this.useProceedResult = opCtx.getUseOperationCallbackProceedReturnResult();
        this.rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
        this.returnValuesArrayForProccedResult = opCtx.getValuesForProceedReturnResult();
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        // For the experiment
        if (useProceedResult) {
            LOGGER.log(LVL, "***** [Index-only experiment] BTREE-SEARCH tryLock count\tS:\t" + proceedSuccessCount
                    + "\tF:\t" + proceedFailCount);
        }
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        super.reset();
        canCallProceedMethod = true;

        // For the experiment
        proceedFailCount = 0;
        proceedSuccessCount = 0;
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();

        //  If useProceed is set to true (e.g., in case of an index-only plan)
        //  and searchCallback.proceed() fail: will add zero - default value
        //                            success: will add one - default value
        if (useProceedResult) {
            tupleBuilderForProceedResult.reset();
            TupleUtils.copyTuple(tupleBuilderForProceedResult, outputElement.getTuple(), cmp.getKeyFieldCount());

            if (!resultOfsearchCallBackProceed) {
                // fail case - add the value that indicates fail.
                tupleBuilderForProceedResult.addField(returnValuesArrayForProccedResult, 0, 5);

                // For the experiment
                proceedFailCount += 1;
            } else {
                // success case - add the value that indicates success.
                tupleBuilderForProceedResult.addField(returnValuesArrayForProccedResult, 5, 5);

                // For the experiment
                proceedSuccessCount += 1;
            }
            copyTuple.reset(tupleBuilderForProceedResult.getFieldEndOffsets(),
                    tupleBuilderForProceedResult.getByteArray());
            outputElement.reset(copyTuple);
        }

        needPushElementIntoQueue = true;
        canCallProceedMethod = false;

    }

    // PriorityQueue can hold one element from each cursor.
    // canCallProceedMethod controls whether we can call proceed() method for this element.
    // i.e. we can return this element if proceed() succeeds.
    // If proceed fails, that is most-likely there is ongoing operations in the in-memory component.
    // After resolving in-memory component issue, we progress again.
    // Also, in order to not release the same element again, we keep the previous output and check it
    // against the current head in the queue.
    protected void checkPriorityQueue() throws HyracksDataException, IndexException {
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                if (canCallProceedMethod) {
                    // call proceed if we can return the element at the head of the queue.
                    resultOfsearchCallBackProceed = searchCallback.proceed(checkElement.getTuple());
                    if (!resultOfsearchCallBackProceed) {
                        // In case proceed() fails and there is an in-memory component,
                        // we can't simply use this element since there might be a change.
                        if (includeMutableComponent) {
                            PriorityQueueElement mutableElement = null;
                            boolean mutableElementFound = false;
                            // scan the PQ for the mutable component's element and delete it
                            // since it can be changed.
                            // (i.e. we can't ensure that the element is the most current one.)
                            Iterator<PriorityQueueElement> it = outputPriorityQueue.iterator();
                            while (it.hasNext()) {
                                mutableElement = it.next();
                                if (mutableElement.getCursorIndex() == 0) {
                                    mutableElementFound = true;
                                    it.remove();
                                    break;
                                }
                            }
                            if (mutableElementFound) {
                                // copy the in-memory tuple
                                if (tupleBuilder == null) {
                                    tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                                }
                                TupleUtils.copyTuple(tupleBuilder, mutableElement.getTuple(), cmp.getKeyFieldCount());

                                copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

                                // unlatch/unpin the leaf page of the index
                                rangeCursors[0].reset();

                                // try to reconcile
                                if (checkElement.getCursorIndex() == 0) {
                                    searchCallback.reconcile(copyTuple);
                                } else {
                                    // If this element is from the disk component, we can call complete() after reconcile()
                                    // since we can guarantee that there is no change.
                                    searchCallback.reconcile(checkElement.getTuple());
                                    searchCallback.complete(checkElement.getTuple());
                                }

                                // re-traverse the index
                                reusablePred.setLowKey(copyTuple, true);
                                btreeAccessors[0].search(rangeCursors[0], reusablePred);
                                boolean isNotExhaustedCursor = pushIntoQueueFromCursorAndReplaceThisElement(mutableElement);

                                if (checkElement.getCursorIndex() == 0) {
                                    if (!isNotExhaustedCursor || cmp.compare(copyTuple, mutableElement.getTuple()) != 0) {
                                        // case: the searched key is no longer exist. We call cancel() to
                                        // reverse the effect of reconcile() method.
                                        searchCallback.cancelReconcile(copyTuple);
                                        continue;
                                    }
                                    //case: the searched key is still there.
                                    //TODO: do we need to call complete() in this case?
                                    //searchCallback.complete(copyTuple);
                                }
                            } else {
                                // the mutable cursor is exhausted and it couldn't find the element.
                                // The failed element did not come from in-memory component.
                                searchCallback.reconcile(checkElement.getTuple());
                            }
                        } else {
                            // proceed failed. However, there is no in-memory component. So just call reconcile.
                            searchCallback.reconcile(checkElement.getTuple());
                        }
                    }
                }

                // If there is no previous tuple or the previous tuple can be ignored
                // This check is needed to not release the same tuple again
                if (outputElement == null) {
                    if (isDeleted(checkElement) && !returnDeletedTuples) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        // Revert the action of proceed()?
                        if (resultOfsearchCallBackProceed) {
                            searchCallback.cancelProceed(checkElement.getTuple());
                        } else {
                            // Revert the action of reconcile()?
                            searchCallback.cancelReconcile(checkElement.getTuple());
                        }
                        needPushElementIntoQueue = true;
                        canCallProceedMethod = false;
                    } else {
                        // All set. This tuple will be fetched when next() is called.
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        PriorityQueueElement e = outputPriorityQueue.poll();
                        pushIntoQueueFromCursorAndReplaceThisElement(e);
                    } else {
                        // If the previous tuple and the head tuple are different
                        // the info of previous tuple is useless
                        if (needPushElementIntoQueue) {
                            pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                            needPushElementIntoQueue = false;
                        }
                        canCallProceedMethod = true;
                        outputElement = null;
                    }
                }

            } else {
                // the priority queue is empty and needPush
                pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                needPushElementIntoQueue = false;
                outputElement = null;
                canCallProceedMethod = true;
            }
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        reusablePred.setLowKeyComparator(cmp);
        reusablePred.setHighKey(predicate.getHighKey(), predicate.isHighKeyInclusive());
        reusablePred.setHighKeyComparator(predicate.getHighKeyComparator());
        includeMutableComponent = false;

        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];

        btreeAccessors = new ITreeIndexAccessor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree;
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                btree = (BTree) ((LSMBTreeMemoryComponent) component).getBTree();
            } else {
                btree = (BTree) ((LSMBTreeDiskComponent) component).getBTree();
            }
            btreeAccessors[i] = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            btreeAccessors[i].search(rangeCursors[i], searchPred);
        }
        setPriorityQueueComparator();
        initPriorityQueue();
        canCallProceedMethod = true;

        // If it is required to use the result of searchCallback.proceed(),
        // we need to initialize the byte array that contains AInt32(0) and AInt32(1).
        //
        if (useProceedResult) {
            //            byte[] tmpResultArray = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
            rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
            //            tupleBuilderForProceedResult = new ArrayTupleBuilder(cmp.getKeyFieldCount() + 1);
            tupleBuilderForProceedResult = new ArrayTupleBuilder(rDescForProceedReturnResult.getFields().length);
            returnValuesArrayForProccedResult = opCtx.getValuesForProceedReturnResult();
            //            ISerializerDeserializer<Object> serializerDeserializerForProceedReturnResult = rDescForProceedReturnResult
            //                    .getFields()[rDescForProceedReturnResult.getFieldCount() - 1];
            //            // INT is 4 byte, however since there is a tag before the actual value,
            //            // we need to provide 5 byte. The serializer is already chosen so the typetag can be anything.
            //            ByteArrayInputStream inStreamZero = new ByteArrayInputStream(tmpResultArray, 0, 5);
            //            ByteArrayInputStream inStreamOne = new ByteArrayInputStream(tmpResultArray, 5, 5);
            //            Object AInt32Zero = serializerDeserializerForProceedReturnResult.deserialize(new DataInputStream(
            //                    inStreamZero));
            //            Object AInt32One = serializerDeserializerForProceedReturnResult
            //                    .deserialize(new DataInputStream(inStreamOne));
            //            ArrayBackedValueStorage castBuffer = new ArrayBackedValueStorage();
            //            serializerDeserializerForProceedReturnResult.serialize(AInt32Zero, castBuffer.getDataOutput());
            //            System.arraycopy(castBuffer.getByteArray(), 0, returnValuesArrayForProccedResult, 0, castBuffer.getLength());
            //            castBuffer.reset();
            //            serializerDeserializerForProceedReturnResult.serialize(AInt32One, castBuffer.getDataOutput());
            //            System.arraycopy(castBuffer.getByteArray(), 0, returnValuesArrayForProccedResult, 5, castBuffer.getLength());
        }

    }
}
