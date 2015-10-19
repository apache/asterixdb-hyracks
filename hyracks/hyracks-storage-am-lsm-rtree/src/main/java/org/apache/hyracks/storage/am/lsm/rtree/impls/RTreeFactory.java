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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IFreePageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class RTreeFactory extends TreeIndexFactory<RTree> {

    public RTreeFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IFreePageManagerFactory freePageManagerFactory, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount) {
        super(bufferCache, fileMapProvider, freePageManagerFactory, interiorFrameFactory, leafFrameFactory,
                cmpFactories, fieldCount);
    }

    @Override
    public RTree createIndexInstance(FileReference file) throws IndexException {
        return new RTree(bufferCache, fileMapProvider, freePageManagerFactory.createFreePageManager(),
                interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
    }

}
