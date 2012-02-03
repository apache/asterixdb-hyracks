/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.btree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeCopyTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeFileNameManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTreeUtils {
    public static LSMBTree createLSMTree(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, String onDiskDir, IBufferCache diskBufferCache,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories) {
        LSMBTreeTupleWriterFactory insertTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, false);
        LSMBTreeTupleWriterFactory deleteTupleWriterFactory = new LSMBTreeTupleWriterFactory(typeTraits,
                cmpFactories.length, true);
        LSMBTreeCopyTupleWriterFactory copyTupleWriterFactory = new LSMBTreeCopyTupleWriterFactory(typeTraits,
                cmpFactories.length);
        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory copyTupleLeafFrameFactory = new BTreeNSMLeafFrameFactory(copyTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(diskBufferCache,
                metaFrameFactory);
        BTreeFactory diskBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, cmpFactories,
                typeTraits.length, interiorFrameFactory, copyTupleLeafFrameFactory);
        BTreeFactory bulkLoadBTreeFactory = new BTreeFactory(diskBufferCache, freePageManagerFactory, cmpFactories,
                typeTraits.length, interiorFrameFactory, insertLeafFrameFactory);
        ILSMFileNameManager fileNameManager = new LSMTreeFileNameManager(onDiskDir);
        LSMBTree lsmTree = new LSMBTree(memBufferCache, memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory, deleteLeafFrameFactory, fileNameManager, diskBTreeFactory,
                bulkLoadBTreeFactory, diskFileMapProvider, typeTraits.length, cmpFactories);
        return lsmTree;
    }
}
