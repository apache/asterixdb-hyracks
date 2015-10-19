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

package org.apache.hyracks.storage.am.lsm.btree.perf;

import org.apache.hyracks.storage.am.common.datagen.DataGenThread;

public interface IExperimentRunner {
    public static int DEFAULT_MAX_OUTSTANDING = 100000;
    
    public void init() throws Exception;
    
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception;
    
    public void reset() throws Exception;
    
    public void deinit() throws Exception;
}
