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

package edu.uci.ics.hyracks.storage.am.lsm.btree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class LSMBTreeTupleWriterFactory extends TypeAwareTupleWriterFactory {

	private static final long serialVersionUID = 1L;
	private final int numKeyFields;
	private final boolean isDelete;
	
	public LSMBTreeTupleWriterFactory(ITypeTraits[] typeTraits, int numKeyFields, boolean isDelete) {
		super(typeTraits);
		this.numKeyFields = numKeyFields;
		this.isDelete = isDelete;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		return new LSMBTreeTupleWriter(typeTraits, numKeyFields, isDelete);
	}
}
