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
package org.apache.hyracks.maven.plugin;

import java.io.File;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * @goal start-cc
 */
public class HyracksCCStartMojo extends AbstractHyracksServerMojo {
    private static final String HYRACKS_CC_SCRIPT = "bin" + File.separator + "hyrackscc";

    /**
     * @parameter property = "port"
     */
    private int port;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        StringBuilder cmdLineBuffer = new StringBuilder();
        if (port != 0) {
            cmdLineBuffer.append("-port ").append(port);
        }
        cmdLineBuffer.append(" -client-net-ip-address 127.0.0.1");
        cmdLineBuffer.append(" -cluster-net-ip-address 127.0.0.1");
        String args = cmdLineBuffer.toString();
        final Process proc = launch(new File(hyracksServerHome, makeScriptName(HYRACKS_CC_SCRIPT)), args, workingDir);
        HyracksServiceRegistry.INSTANCE.addServiceProcess(proc);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
