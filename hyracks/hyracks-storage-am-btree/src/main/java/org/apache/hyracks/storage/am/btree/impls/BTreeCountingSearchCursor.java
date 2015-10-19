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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class BTreeCountingSearchCursor implements ITreeIndexCursor {

    private int fileId = -1;
    private ICachedPage page = null;
    private IBufferCache bufferCache = null;

    private int tupleIndex = 0;
    private int stopTupleIndex;
    private int count = -1;

    private FindTupleMode lowKeyFtm;
    private FindTupleMode highKeyFtm;

    private FindTupleNoExactMatchPolicy lowKeyFtp;
    private FindTupleNoExactMatchPolicy highKeyFtp;

    private final IBTreeLeafFrame frame;
    private final ITreeIndexTupleReference frameTuple;
    private final boolean exclusiveLatchNodes;
    private boolean isPageDirty;

    private RangePredicate pred;
    private MultiComparator lowKeyCmp;
    private MultiComparator highKeyCmp;
    private ITupleReference lowKey;
    private ITupleReference highKey;

    // For storing the count.
    private byte[] countBuf = new byte[4];
    private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
    private ArrayTupleReference countTuple = new ArrayTupleReference();

    public BTreeCountingSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference();
        this.exclusiveLatchNodes = exclusiveLatchNodes;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page != null) {
            if (exclusiveLatchNodes) {
                page.releaseWriteLatch(isPageDirty);
            } else {
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
        }

        page = ((BTreeCursorInitialState) initialState).getPage();
        isPageDirty = false;
        frame.setPage(page);

        pred = (RangePredicate) searchPred;
        lowKeyCmp = pred.getLowKeyComparator();
        highKeyCmp = pred.getHighKeyComparator();

        lowKey = pred.getLowKey();
        highKey = pred.getHighKey();

        // init
        lowKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.lowKeyInclusive) {
            lowKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        } else {
            lowKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        }

        highKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.highKeyInclusive) {
            highKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        } else {
            highKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        }

        tupleIndex = getLowKeyIndex();
        stopTupleIndex = getHighKeyIndex();
    }

    private void fetchNextLeafPage(int nextLeafPage) throws HyracksDataException {
        do {
            ICachedPage nextLeaf = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeafPage), false);
            if (exclusiveLatchNodes) {
                nextLeaf.acquireWriteLatch();
                page.releaseWriteLatch(isPageDirty);
            } else {
                nextLeaf.acquireReadLatch();
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
            page = nextLeaf;
            isPageDirty = false;
            frame.setPage(page);
            nextLeafPage = frame.getNextLeaf();
        } while (frame.getTupleCount() == 0 && nextLeafPage > 0);
    }

    private int getLowKeyIndex() throws HyracksDataException {
        if (lowKey == null) {
            return 0;
        }
        int index = frame.findTupleIndex(lowKey, frameTuple, lowKeyCmp, lowKeyFtm, lowKeyFtp);
        if (pred.lowKeyInclusive) {
            index++;
        } else {
            if (index < 0) {
                index = frame.getTupleCount();
            }
        }
        return index;
    }

    private int getHighKeyIndex() throws HyracksDataException {
        if (highKey == null) {
            return frame.getTupleCount() - 1;
        }
        int index = frame.findTupleIndex(highKey, frameTuple, highKeyCmp, highKeyFtm, highKeyFtp);
        if (pred.highKeyInclusive) {
            if (index < 0) {
                index = frame.getTupleCount() - 1;
            } else {
                index--;
            }
        }
        return index;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        // get the count for the current page
        // follow the sibling pointer until last page
        // if no more tuples on a page, then done

        if (count < 0) {
            count = 0;

            while (stopTupleIndex >= 0 || frame.getTupleCount() == 0) {
                count += (stopTupleIndex - tupleIndex + 1);

                int nextLeafPage = frame.getNextLeaf();
                if (nextLeafPage >= 0) {
                    fetchNextLeafPage(nextLeafPage);
                } else {
                    // No more pages. Done counting!
                    break;
                }

                tupleIndex = 0;
                stopTupleIndex = getHighKeyIndex();
            }

            return true;
        }

        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        // Do nothing. Count is performed just once!
        IntegerPointable.setInteger(countBuf, 0, count);
        tupleBuilder.addField(countBuf, 0, 4);
        countTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    @Override
    public void close() throws HyracksDataException {
        if (page != null) {
            if (exclusiveLatchNodes) {
                page.releaseWriteLatch(isPageDirty);
            } else {
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
        }
        tupleBuilder.reset();
        tupleIndex = 0;
        page = null;
        isPageDirty = false;
        pred = null;
        count = -1;
    }

    @Override
    public void reset() {
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ITupleReference getTuple() {
        return countTuple;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return exclusiveLatchNodes;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        if (exclusiveLatchNodes) {
            isPageDirty = true;
        } else {
            throw new HyracksDataException("This cursor has not been created with the intention to allow updates.");
        }
    }

}
