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
package org.apache.hyracks.dataflow.std.union;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;

public class UnionAllOperatorDescriptor extends AbstractOperatorDescriptor {
    public UnionAllOperatorDescriptor(IOperatorDescriptorRegistry spec, int nInputs, RecordDescriptor recordDescriptor) {
        super(spec, nInputs, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        UnionActivityNode uba = new UnionActivityNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, uba);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, uba, i);
        }
        builder.addTargetEdge(0, uba, 0);
    }

    private class UnionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public UnionActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new UnionOperator(ctx, inRecordDesc);
        }
    }

    private class UnionOperator extends AbstractUnaryOutputOperatorNodePushable {
        private int nOpened;

        private int nClosed;

        private boolean failed;

        public UnionOperator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc) {
            nOpened = 0;
            nClosed = 0;
        }

        @Override
        public int getInputArity() {
            return inputArity;
        }

        @Override
        public IFrameWriter getInputFrameWriter(int index) {
            return new IFrameWriter() {
                @Override
                public void open() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (++nOpened == 1) {
                            writer.open();
                        }
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        writer.nextFrame(buffer);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (failed) {
                            writer.fail();
                        }
                        failed = true;
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (++nClosed == inputArity) {
                            writer.close();
                        }
                    }
                }
            };
        }
    }
}