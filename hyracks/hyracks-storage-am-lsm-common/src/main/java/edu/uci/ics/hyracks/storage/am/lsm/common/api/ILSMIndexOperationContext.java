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

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;

public interface ILSMIndexOperationContext extends IIndexOperationContext {
    public List<ILSMComponent> getComponentHolder();

    public List<ILSMComponent> getComponentsToBeMerged();

    public ISearchOperationCallback getSearchOperationCallback();

    public IModificationOperationCallback getModificationCallback();

    public void setCurrentMutableComponentId(int currentMutableComponentId);

    public void setSearchPredicate(ISearchPredicate searchPredicate);

    public ISearchPredicate getSearchPredicate();

    public List<ILSMComponent> getComponentsToBeReplicated();

    // If set to true, a search operation will generate one more field in addition to the index search output.
    // That is, the result of SearchOperationCallback.proceed() will be recorded.
    // For this, we also need to have record descriptor information.
    public void setUseOperationCallbackProceedReturnResult(boolean useOperationCallbackProceedReturnResult);

    public boolean getUseOperationCallbackProceedReturnResult();

    public void setRecordDescForProceedReturnResult(RecordDescriptor rDescForProceedReturnResult);

    public RecordDescriptor getRecordDescForProceedReturnResult();

    public void setValuesForProceedReturnResult(byte[] valuesForProceedReturnResult);

    public byte[] getValuesForProceedReturnResult();

}
