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
package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;

public interface ILSMComponentFilterManager {

    public void updateFilterInfo(ILSMComponentFilter filter, List<ITupleReference> filterTuples)
            throws HyracksDataException;

    public boolean readFilterInfo(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException;

    public void writeFilterInfo(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException;

}
