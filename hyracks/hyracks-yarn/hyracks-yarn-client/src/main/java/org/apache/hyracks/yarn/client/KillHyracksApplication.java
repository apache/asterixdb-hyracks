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
package org.apache.hyracks.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.hyracks.yarn.common.protocols.clientrm.YarnClientRMConnection;

public class KillHyracksApplication {
    private final Options options;

    private KillHyracksApplication(Options options) {
        this.options = options;
    }

    private void run() throws Exception {
        Configuration conf = new Configuration();
        YarnConfiguration yconf = new YarnConfiguration(conf);
        YarnClientRMConnection crmc = new YarnClientRMConnection(yconf);
        crmc.killApplication(options.appId);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        new KillHyracksApplication(options).run();
    }

    private static class Options {
        @Option(name = "-application-id", required = true, usage = "Application Id")
        String appId;
    }
}