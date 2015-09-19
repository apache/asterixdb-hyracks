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
package org.apache.hyracks.control.cc.work;

import java.util.List;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobStatusConditionVariable;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class WaitForJobCompletionWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private final IResultCallback<Object> callback;

    public WaitForJobCompletionWork(ClusterControllerService ccs, JobId jobId, IResultCallback<Object> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        final IJobStatusConditionVariable cRunningVar = ccs.getActiveRunMap().get(jobId);
        if (cRunningVar != null) {
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        cRunningVar.waitForCompletion();
                        callback.setValue(null);
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } else {
            final IJobStatusConditionVariable cArchivedVar = ccs.getRunMapArchive().get(jobId);
            if (cArchivedVar != null) {
                ccs.getExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cArchivedVar.waitForCompletion();
                            callback.setValue(null);
                        } catch (Exception e) {
                            callback.setException(e);
                        }
                    }
                });
            } else {
                final List<Exception> exceptions = ccs.getRunHistory().get(jobId);
                ccs.getExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        callback.setValue(null);
                        if (exceptions != null && exceptions.size() > 0) {
                            /** only report the first exception because IResultCallback will only throw one exception anyway */
                            callback.setException(exceptions.get(0));
                        }
                    }
                });
            }
        }
    }
}