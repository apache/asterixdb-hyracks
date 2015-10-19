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

package org.apache.hyracks.dataflow.common.data.accessors;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;

public final class FrameTupleReference implements IFrameTupleReference {
    private IFrameTupleAccessor fta;
    private int tIndex;

    public void reset(IFrameTupleAccessor fta, int tIndex) {
        this.fta = fta;
        this.tIndex = tIndex;
    }

    @Override
    public IFrameTupleAccessor getFrameTupleAccessor() {
        return fta;
    }

    @Override
    public int getTupleIndex() {
        return tIndex;
    }

    @Override
    public int getFieldCount() {
        return fta.getFieldCount();
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fta.getBuffer().array();
    }

    @Override
    public int getFieldStart(int fIdx) {
        return fta.getTupleStartOffset(tIndex) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(tIndex, fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return fta.getFieldLength(tIndex, fIdx);
    }
}