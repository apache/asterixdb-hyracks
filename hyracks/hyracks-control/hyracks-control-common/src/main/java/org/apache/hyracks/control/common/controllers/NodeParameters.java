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
package org.apache.hyracks.control.common.controllers;

import java.io.Serializable;

import org.apache.hyracks.api.client.ClusterControllerInfo;

public class NodeParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    private ClusterControllerInfo ccInfo;

    private Serializable distributedState;

    private int heartbeatPeriod;

    private int profileDumpPeriod;

    public ClusterControllerInfo getClusterControllerInfo() {
        return ccInfo;
    }

    public void setClusterControllerInfo(ClusterControllerInfo ccInfo) {
        this.ccInfo = ccInfo;
    }

    public Serializable getDistributedState() {
        return distributedState;
    }

    public void setDistributedState(Serializable distributedState) {
        this.distributedState = distributedState;
    }

    public int getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public void setHeartbeatPeriod(int heartbeatPeriod) {
        this.heartbeatPeriod = heartbeatPeriod;
    }

    public int getProfileDumpPeriod() {
        return profileDumpPeriod;
    }

    public void setProfileDumpPeriod(int profileDumpPeriod) {
        this.profileDumpPeriod = profileDumpPeriod;
    }
}