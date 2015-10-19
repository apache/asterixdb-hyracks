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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;

public class JobInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final JobId jobId;

    private final JobStatus status;

    private final Map<OperatorDescriptorId, Map<Integer, String>> operatorLocations;

    public JobInfo(JobId jobId, JobStatus jobStatus, Map<OperatorDescriptorId, Map<Integer, String>> operatorLocations) {
        this.jobId = jobId;
        this.operatorLocations = operatorLocations;
        this.status = jobStatus;
    }

    public JobId getJobId() {
        return jobId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Map<OperatorDescriptorId, Map<Integer, String>> getOperatorLocations() {
        return operatorLocations;
    }

}
