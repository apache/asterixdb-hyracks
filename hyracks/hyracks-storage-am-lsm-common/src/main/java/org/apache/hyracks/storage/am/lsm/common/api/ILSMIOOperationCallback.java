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

public interface ILSMIOOperationCallback {

    /**
     * This method is called on an IO operation sometime before the operation is executed.
     * (i.e. IO operations could be flush or merge operations.)
     */
    public void beforeOperation(LSMOperationType opType) throws HyracksDataException;

    /**
     * This method is called on an IO operation sometime after the operation was completed.
     * (i.e. IO operations could be flush or merge operations.)
     * 
     * @param oldComponents
     * @param newComponent
     * @throws HyracksDataException
     */
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException;

    /**
     * This method is called on an IO operation when the operation needs any cleanup works
     * regardless that the IO operation was executed or not. Once the IO operation is executed,
     * this method should be called after ILSMIOOperationCallback.afterOperation() was called.
     * 
     * @param newComponent
     * @throws HyracksDataException
     */
    public void afterFinalize(LSMOperationType opType, ILSMComponent newComponent) throws HyracksDataException;

    public void setNumOfMutableComponents(int count);
}
