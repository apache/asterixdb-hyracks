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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAppender;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class LocalityAwarePartitionDataWriter implements IFrameWriter {

    private final IFrameWriter[] pWriters;
    private final IFrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;

    public LocalityAwarePartitionDataWriter(IHyracksTaskContext ctx, IPartitionWriterFactory pwFactory,
            RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc, int nConsumerPartitions,
            ILocalityMap localityMap, int senderIndex) throws HyracksDataException {
        int[] consumerPartitions = localityMap.getConsumers(senderIndex, nConsumerPartitions);
        pWriters = new IFrameWriter[consumerPartitions.length];
        appenders = new IFrameTupleAppender[consumerPartitions.length];
        for (int i = 0; i < consumerPartitions.length; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(consumerPartitions[i]);
                appenders[i] = new FrameTupleAppender();
                appenders[i].reset(new VSizeFrame(ctx), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.tpc = tpc;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int h = pWriters.length == 1 ? 0 : tpc.partition(tupleAccessor, i, pWriters.length);
            FrameUtils.appendToWriter(pWriters[h], appenders[h], tupleAccessor, i);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            pWriters[i].fail();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            appenders[i].flush(pWriters[i], true);
            pWriters[i].close();
        }
    }

}
