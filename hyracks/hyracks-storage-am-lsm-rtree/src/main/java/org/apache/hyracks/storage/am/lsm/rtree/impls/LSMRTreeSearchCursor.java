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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMRTreeSearchCursor extends LSMRTreeAbstractCursor {

    private int currentCursor;
    private PermutingTupleReference btreeTuple;
    private boolean useProceedResult = false;
    private RecordDescriptor rDescForProceedReturnResult = null;
    private byte[] valuesForOperationCallbackProceedReturnResult;
    private boolean resultOfsearchCallBackProceed = false;
    private int numberOfFieldFromIndex = 0;
    private ArrayTupleBuilder tupleBuilderForProceedResult;
    //    private byte[] returnValuesArrayForProccedResult = new byte[10];
    private ArrayTupleReference copyTuple = null;

    // For the experiment
    protected int proceedFailCount = 0;
    protected int proceedSuccessCount = 0;
    private static final Logger LOGGER = Logger.getLogger(LSMRTreeSearchCursor.class.getName());
    private static final Level LVL = Level.WARNING;

    public LSMRTreeSearchCursor(ILSMIndexOperationContext opCtx, int[] buddyBTreeFields) {
        super(opCtx);
        currentCursor = 0;
        this.btreeTuple = new PermutingTupleReference(buddyBTreeFields);
        this.useProceedResult = opCtx.getUseOperationCallbackProceedReturnResult();
        this.rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
        this.valuesForOperationCallbackProceedReturnResult = opCtx.getValuesForProceedReturnResult();
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        currentCursor = 0;
        // For the experiment
        if (useProceedResult) {
            LOGGER.log(LVL, "***** [Index-only experiment] RTREE-SEARCH tryLock count\tS:\t" + proceedSuccessCount
                    + "\tF:\t" + proceedFailCount);
        }
    }

    @Override
    public void reset() throws HyracksDataException {
        if (!open) {
            return;
        }

        currentCursor = 0;
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].close();
                btreeCursors[i].close();
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }

        // For the experiment
        proceedFailCount = 0;
        proceedSuccessCount = 0;
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursor < numberOfTrees) {
            rtreeCursors[currentCursor].reset();
            try {
                rtreeAccessors[currentCursor].search(rtreeCursors[currentCursor], rtreeSearchPredicate);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (foundNext) {
            return true;
        }
        while (currentCursor < numberOfTrees) {
            while (rtreeCursors[currentCursor].hasNext()) {
                rtreeCursors[currentCursor].next();
                ITupleReference currentTuple = rtreeCursors[currentCursor].getTuple();
                // TODO: at this point, we only add proceed() and cancelProceed() part.
                // reconcile() and complete() can be added later after considering the semantics.

                // Call proceed() to do necessary operations before returning this tuple.
                resultOfsearchCallBackProceed = searchCallback.proceed(currentTuple);
                btreeTuple.reset(rtreeCursors[currentCursor].getTuple());
                boolean killerTupleFound = false;
                for (int i = 0; i < currentCursor; i++) {
                    btreeCursors[i].reset();
                    btreeRangePredicate.setHighKey(btreeTuple, true);
                    btreeRangePredicate.setLowKey(btreeTuple, true);
                    btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                    try {
                        if (btreeCursors[i].hasNext()) {
                            killerTupleFound = true;
                            break;
                        }
                    } finally {
                        btreeCursors[i].close();
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    foundNext = true;
                    return true;
                } else {
                    // need to reverse the effect of proceed() since we can't return this tuple.
                    if (resultOfsearchCallBackProceed) {
                        searchCallback.cancelProceed(currentTuple);
                    }
                }
            }
            rtreeCursors[currentCursor].close();
            currentCursor++;
            searchNextCursor();
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;

        //  If useProceed is set to true (e.g., in case of an index-only plan)
        //  and searchCallback.proceed() fail: will add zero - default value
        //                            success: will add one - default value
        if (useProceedResult) {
            tupleBuilderForProceedResult.reset();
            TupleUtils.copyTuple(tupleBuilderForProceedResult, frameTuple, numberOfFieldFromIndex);

            if (!resultOfsearchCallBackProceed) {
                // fail case - add the value that indicates fail.
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 0, 5);

                // For the experiment
                proceedFailCount += 1;
            } else {
                // success case - add the value that indicates success.
                tupleBuilderForProceedResult.addField(valuesForOperationCallbackProceedReturnResult, 5, 5);

                // For the experiment
                proceedSuccessCount += 1;
            }
            copyTuple.reset(tupleBuilderForProceedResult.getFieldEndOffsets(),
                    tupleBuilderForProceedResult.getByteArray());
            frameTuple = copyTuple;
        }

    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.open(initialState, searchPred);
        searchNextCursor();

        numberOfFieldFromIndex = btreeCmp.getKeyFieldCount()
                + rtreeSearchPredicate.getLowKeyComparator().getKeyFieldCount();

        // If it is required to use the result of searchCallback.proceed(),
        // we need to initialize the byte array that contains true and false result.
        //
        if (useProceedResult) {
            //            returnValuesArrayForProccedResult =
            //            byte[] tmpResultArray = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
            //            rDescForProceedReturnResult = opCtx.getRecordDescForProceedReturnResult();
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

            tupleBuilderForProceedResult = new ArrayTupleBuilder(numberOfFieldFromIndex + 1);
            copyTuple = new ArrayTupleReference();

        }
    }

}