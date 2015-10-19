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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMBTreeWithBuddyDiskComponentFactory implements
		ILSMComponentFactory {

	private final TreeIndexFactory<BTree> btreeFactory;
	private final TreeIndexFactory<BTree> buddyBtreeFactory;
	private final BloomFilterFactory bloomFilterFactory;

	public LSMBTreeWithBuddyDiskComponentFactory(
			TreeIndexFactory<BTree> btreeFactory,
			TreeIndexFactory<BTree> buddyBtreeFactory,
			BloomFilterFactory bloomFilterFactory) {
		this.btreeFactory = btreeFactory;
		this.buddyBtreeFactory = buddyBtreeFactory;
		this.bloomFilterFactory = bloomFilterFactory;
	}

	@Override
	public ILSMComponent createLSMComponentInstance(
			LSMComponentFileReferences cfr) throws IndexException,
			HyracksDataException {
		return new LSMBTreeWithBuddyDiskComponent(
				btreeFactory.createIndexInstance(cfr
						.getInsertIndexFileReference()),
				buddyBtreeFactory.createIndexInstance(cfr
						.getDeleteIndexFileReference()),
				bloomFilterFactory.createBloomFiltertInstance(cfr
						.getBloomFilterFileReference()));
	}

	@Override
	public IBufferCache getBufferCache() {
		return btreeFactory.getBufferCache();
	}

}
