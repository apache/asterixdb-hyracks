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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;

// An interface containing the new methods introduced for bulk transactions
public interface ITwoPCIndex {
    /**
     * This function is used to create a BulkLoader for a transaction that is capable of insertions and deletions
     * and the bulk loaded component is hidden from the index
     */
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException;
    
    /**
     * This function is used to commit the previous transaction if it was resulted in creating any components
     */
    public void commitTransaction() throws TreeIndexException, HyracksDataException, IndexException;
    
    /**
     * This function is used to abort the last transaction
     */
    public void abortTransaction() throws TreeIndexException;
    
    /**
     * This function is used to recover a transaction if the system crashed after the decision to commit
     */
    public void recoverTransaction() throws TreeIndexException;
    
    /**
     * This function is used to add the committed disk component to the appropriate list and reflect the changes
     */
    public void commitTransactionDiskComponent(ILSMComponent newComponent) throws IndexException, HyracksDataException;
    
    /**
     * This function is used to create a version specific accessor to search a specific version
     */
    public ILSMIndexAccessorInternal createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion) throws HyracksDataException;
    
    /**
     * This function is used to get the first components list
     */
    public List<ILSMComponent> getFirstComponentList();
    
    /**
     * This function is used to get teh second components list
     */
    public List<ILSMComponent> getSecondComponentList();
    
    /**
     * This function is used to get the current version id of the index
     */
    public int getCurrentVersion();
}
