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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMInvertedIndexDiskComponentFactory implements ILSMComponentFactory {
    private final OnDiskInvertedIndexFactory diskInvIndexFactory;
    private final TreeIndexFactory<BTree> btreeFactory;
    private final BloomFilterFactory bloomFilterFactory;
    private final ILSMComponentFilterFactory filterFactory;

    public LSMInvertedIndexDiskComponentFactory(OnDiskInvertedIndexFactory diskInvIndexFactory,
            TreeIndexFactory<BTree> btreeFactory, BloomFilterFactory bloomFilterFactory,
            ILSMComponentFilterFactory filterFactory) {
        this.diskInvIndexFactory = diskInvIndexFactory;
        this.btreeFactory = btreeFactory;
        this.bloomFilterFactory = bloomFilterFactory;
        this.filterFactory = filterFactory;
    }

    @Override
    public ILSMComponent createLSMComponentInstance(LSMComponentFileReferences cfr) throws IndexException,
            HyracksDataException {
        return new LSMInvertedIndexDiskComponent(diskInvIndexFactory.createIndexInstance(cfr
                .getInsertIndexFileReference()), btreeFactory.createIndexInstance(cfr.getDeleteIndexFileReference()),
                bloomFilterFactory.createBloomFiltertInstance(cfr.getBloomFilterFileReference()),
                filterFactory == null ? null : filterFactory.createLSMComponentFilter());
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskInvIndexFactory.getBufferCache();
    }
}
