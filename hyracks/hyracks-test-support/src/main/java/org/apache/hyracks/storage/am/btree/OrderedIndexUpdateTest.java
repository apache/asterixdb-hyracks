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

package org.apache.hyracks.storage.am.btree;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexUpdateTest extends OrderedIndexTestDriver {

    private final OrderedIndexTestUtils orderedIndexTestUtils;

    public OrderedIndexUpdateTest(BTreeLeafFrameType[] leafFrameTypesToTest) {
        super(leafFrameTypesToTest);
        this.orderedIndexTestUtils = new OrderedIndexTestUtils();
    }

    private static final int numUpdateRounds = AccessMethodTestsConfig.BTREE_NUM_UPDATE_ROUNDS;

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey, ITupleReference prefixHighKey)
            throws Exception {
        // This is a noop because we can only update non-key fields.
        if (fieldSerdes.length == numKeys) {
            return;
        }
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType);
        ctx.getIndex().create();
        ctx.getIndex().activate();
        // We assume all fieldSerdes are of the same type. Check the first one
        // to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            orderedIndexTestUtils.insertIntTuples(ctx, numTuplesToInsert, getRandom());
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            orderedIndexTestUtils.insertStringTuples(ctx, numTuplesToInsert, getRandom());
        }
        int numTuplesPerDeleteRound = (int) Math.ceil((float) ctx.getCheckTuples().size() / (float) numUpdateRounds);
        for (int j = 0; j < numUpdateRounds; j++) {
            orderedIndexTestUtils.updateTuples(ctx, numTuplesPerDeleteRound, getRandom());
            orderedIndexTestUtils.checkPointSearches(ctx);
            orderedIndexTestUtils.checkScan(ctx);
            orderedIndexTestUtils.checkDiskOrderScan(ctx);
            orderedIndexTestUtils.checkRangeSearch(ctx, lowKey, highKey, true, true);
            if (prefixLowKey != null && prefixHighKey != null) {
                orderedIndexTestUtils.checkRangeSearch(ctx, prefixLowKey, prefixHighKey, true, true);
            }
        }

        ctx.getIndex().validate();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "Update";
    }
}
