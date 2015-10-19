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
package org.apache.hyracks.algebricks.data.impl;

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

public class BinaryIntegerInspectorImpl implements IBinaryIntegerInspector {
    public static final IBinaryIntegerInspectorFactory FACTORY = new IBinaryIntegerInspectorFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryIntegerInspector createBinaryIntegerInspector(IHyracksTaskContext ctx) {
            return new BinaryIntegerInspectorImpl();
        }
    };

    private BinaryIntegerInspectorImpl() {
    }

    @Override
    public int getIntegerValue(byte[] bytes, int offset, int length) {
        return IntegerPointable.getInteger(bytes, offset);
    }
}