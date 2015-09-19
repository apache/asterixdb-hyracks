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
package org.apache.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class LimitOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int outputLimit;

    public LimitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputLimit) {
        super(spec, 1, 1);
        recordDescriptors[0] = rDesc;
        this.outputLimit = outputLimit;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private FrameTupleAccessor fta;
            private int currentSize;
            private boolean finished;

            @Override
            public void open() throws HyracksDataException {
                fta = new FrameTupleAccessor(recordDescriptors[0]);
                currentSize = 0;
                finished = false;
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (!finished) {
                    fta.reset(buffer);
                    int count = fta.getTupleCount();
                    if ((currentSize + count) > outputLimit) {
                        FrameTupleAppender partialAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                        int copyCount = outputLimit - currentSize;
                        for (int i = 0; i < copyCount; i++) {
                            FrameUtils.appendToWriter(writer, partialAppender, fta, i);
                            currentSize++;
                        }
                        partialAppender.flush(writer,false);
                        finished = true;
                    } else {
                        FrameUtils.flushFrame(buffer, writer);
                        currentSize += count;
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();

            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
            }
        };
    }

}
