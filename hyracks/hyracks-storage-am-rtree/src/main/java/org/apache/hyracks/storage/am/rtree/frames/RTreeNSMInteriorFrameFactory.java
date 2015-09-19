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

package org.apache.hyracks.storage.am.rtree.frames;

import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;

public class RTreeNSMInteriorFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriterFactory tupleWriterFactory;
    private final IPrimitiveValueProviderFactory[] keyValueProviderFactories;
    private final RTreePolicyType rtreePolicyType;

    public RTreeNSMInteriorFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory,
            IPrimitiveValueProviderFactory[] keyValueProviderFactories, RTreePolicyType rtreePolicyType) {
        this.tupleWriterFactory = tupleWriterFactory;
        if (keyValueProviderFactories.length % 2 != 0) {
            throw new IllegalArgumentException("The key has different number of dimensions.");
        }
        this.keyValueProviderFactories = keyValueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
    }

    @Override
    public IRTreeInteriorFrame createFrame() {
        IPrimitiveValueProvider[] keyValueProviders = new IPrimitiveValueProvider[keyValueProviderFactories.length];
        for (int i = 0; i < keyValueProviders.length; i++) {
            keyValueProviders[i] = keyValueProviderFactories[i].createPrimitiveValueProvider();
        }
        return new RTreeNSMInteriorFrame(tupleWriterFactory.createTupleWriter(), keyValueProviders, rtreePolicyType);
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }
}
