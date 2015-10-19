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
package org.apache.hyracks.control.nc.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.comm.channels.IChannelConnectionFactory;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;
import org.apache.hyracks.net.buffers.ICloseableBufferAcceptor;
import org.apache.hyracks.net.exceptions.NetException;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.IChannelOpenListener;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemux;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;

public class DatasetNetworkManager implements IChannelConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(DatasetNetworkManager.class.getName());

    private static final int MAX_CONNECTION_ATTEMPTS = 5;

    static final int INITIAL_MESSAGE_SIZE = 20;

    private final IDatasetPartitionManager partitionManager;

    private final MuxDemux md;

    private final int nBuffers;

    private NetworkAddress localNetworkAddress;

    private NetworkAddress publicNetworkAddress;

    /**
     * @param inetAddress - Internet address to bind the listen port to
     * @param inetPort - Port to bind on inetAddress
     * @param publicInetAddress - Internet address to report to consumers;
     *    useful when behind NAT. null = same as inetAddress
     * @param publicInetPort - Port to report to consumers; useful when
     *    behind NAT. Ignored if publicInetAddress is null. 0 = same as inetPort
     */
    public DatasetNetworkManager(String inetAddress, int inetPort, IDatasetPartitionManager partitionManager, int nThreads,
                                 int nBuffers, String publicInetAddress, int publicInetPort) throws IOException {
        this.partitionManager = partitionManager;
        this.nBuffers = nBuffers;
        md = new MuxDemux(new InetSocketAddress(inetAddress, inetPort), new ChannelOpenListener(), nThreads,
                MAX_CONNECTION_ATTEMPTS);
        // Just save these values for the moment; may be reset in start()
        publicNetworkAddress = new NetworkAddress(publicInetAddress, publicInetPort);
    }

    public void start() throws IOException {
        md.start();
        InetSocketAddress sockAddr = md.getLocalAddress();
        localNetworkAddress = new NetworkAddress(sockAddr.getHostString(), sockAddr.getPort());

        // See if the public address was explicitly specified, and if not,
        // make it a copy of localNetworkAddress
        if (publicNetworkAddress.getAddress() == null) {
            publicNetworkAddress = localNetworkAddress;
        }
        else {
            // Likewise for public port
            if (publicNetworkAddress.getPort() == 0) {
                publicNetworkAddress = new NetworkAddress
                    (publicNetworkAddress.getAddress(), sockAddr.getPort());
            }
        }
    }

    public NetworkAddress getLocalNetworkAddress() {
        return localNetworkAddress;
    }

    public NetworkAddress getPublicNetworkAddress() {
        return publicNetworkAddress;
    }

    public void stop() {

    }

    public ChannelControlBlock connect(SocketAddress remoteAddress) throws InterruptedException, NetException {
        MultiplexedConnection mConn = md.connect((InetSocketAddress) remoteAddress);
        return mConn.openChannel();
    }

    private class ChannelOpenListener implements IChannelOpenListener {
        @Override
        public void channelOpened(ChannelControlBlock channel) {
            channel.getReadInterface().setFullBufferAcceptor(new InitialBufferAcceptor(channel));
            channel.getReadInterface().getEmptyBufferAcceptor().accept(ByteBuffer.allocate(INITIAL_MESSAGE_SIZE));
        }
    }

    private class InitialBufferAcceptor implements ICloseableBufferAcceptor {
        private final ChannelControlBlock ccb;

        private NetworkOutputChannel noc;

        public InitialBufferAcceptor(ChannelControlBlock ccb) {
            this.ccb = ccb;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            JobId jobId = new JobId(buffer.getLong());
            ResultSetId rsId = new ResultSetId(buffer.getLong());
            int partition = buffer.getInt();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Received initial dataset partition read request for JobId: " + jobId + " partition: "
                        + partition + " on channel: " + ccb);
            }
            noc = new NetworkOutputChannel(ccb, nBuffers);
            try {
                partitionManager.initializeDatasetPartitionReader(jobId, rsId, partition, noc);
            } catch (HyracksException e) {
                noc.abort();
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void error(int ecode) {
            if (noc != null) {
                noc.abort();
            }
        }
    }

    public MuxDemuxPerformanceCounters getPerformanceCounters() {
        return md.getPerformanceCounters();
    }
}
