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

package org.apache.hyracks.storage.am.lsm.invertedindex.inmemory;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTokenizingTupleIterator;

public class InMemoryInvertedIndexOpContext implements IIndexOperationContext {
    public IndexOperation op;
    public final BTree btree;

    // Needed for search operations,    
    public RangePredicate btreePred;
    public BTreeAccessor btreeAccessor;
    public MultiComparator btreeCmp;
    public IBinaryComparatorFactory[] tokenCmpFactories;
    public MultiComparator tokenFieldsCmp;

    // To generate in-memory BTree tuples for insertions.
    protected final IBinaryTokenizerFactory tokenizerFactory;
    public InvertedIndexTokenizingTupleIterator tupleIter;

    public InMemoryInvertedIndexOpContext(BTree btree, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory) {
        this.btree = btree;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        switch (newOp) {
            case INSERT:
            case DELETE: {
                if (tupleIter == null) {
                    setTokenizingTupleIterator();
                }
                break;
            }
            case SEARCH: {
                if (btreePred == null) {
                    btreePred = new RangePredicate(null, null, true, true, null, null);
                    btreeAccessor = (BTreeAccessor) btree.createAccessor(NoOpOperationCallback.INSTANCE,
                            NoOpOperationCallback.INSTANCE);
                    btreeCmp = MultiComparator.create(btree.getComparatorFactories());
                    tokenFieldsCmp = MultiComparator.create(tokenCmpFactories);
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("Unsupported operation " + newOp);
            }
        }
        op = newOp;
    }

    @Override
    public void reset() {
        op = null;
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    protected void setTokenizingTupleIterator() {
        IBinaryTokenizer tokenizer = tokenizerFactory.createTokenizer();
        tupleIter = new InvertedIndexTokenizingTupleIterator(tokenCmpFactories.length, btree.getFieldCount()
                - tokenCmpFactories.length, tokenizer);
    }
}
