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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractMemoryLSMComponent;
import org.apache.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeMemoryComponent extends AbstractMemoryLSMComponent {

    private final RTree rtree;
    private final BTree btree;

    public LSMRTreeMemoryComponent(RTree rtree, BTree btree, IVirtualBufferCache vbc, boolean isActive,
            ILSMComponentFilter filter) {
        super(vbc, isActive, filter);
        this.rtree = rtree;
        this.btree = btree;
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

    @Override
    protected void reset() throws HyracksDataException {
        super.reset();
        rtree.deactivate();
        rtree.destroy();
        rtree.create();
        rtree.activate();
        if (btree != null) {
            btree.deactivate();
            btree.destroy();
            btree.create();
            btree.activate();
        }
    }

}
