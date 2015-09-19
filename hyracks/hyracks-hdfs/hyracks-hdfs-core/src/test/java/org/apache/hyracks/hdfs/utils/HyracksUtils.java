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

package org.apache.hyracks.hdfs.utils;

import java.util.EnumSet;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public class HyracksUtils {

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    public static final int DEFAULT_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_CLIENT_PORT = 2099;
    public static final String CC_HOST = "localhost";

    public static final int FRAME_SIZE = 65536;

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = CC_HOST;
        ccConfig.clusterNetIpAddress = CC_HOST;
        ccConfig.clusterNetPort = TEST_HYRACKS_CC_PORT;
        ccConfig.clientNetPort = TEST_HYRACKS_CC_CLIENT_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 0;
        ccConfig.profileDumpPeriod = -1;

        // cluster controller
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // two node controllers
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.clusterNetIPAddress = "localhost";
        ncConfig1.ccPort = TEST_HYRACKS_CC_PORT;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.resultIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.clusterNetIPAddress = "localhost";
        ncConfig2.ccPort = TEST_HYRACKS_CC_PORT;
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.resultIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        // hyracks connection
        hcc = new HyracksConnection(CC_HOST, TEST_HYRACKS_CC_CLIENT_PORT);
    }

    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    public static void runJob(JobSpecification spec, String appName) throws Exception {
        spec.setFrameSize(FRAME_SIZE);
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
    }

}
