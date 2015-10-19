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
package org.apache.hyracks.control.common.job.profiling.om;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.io.IWritable;

public abstract class AbstractProfile implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    protected Map<String, Long> counters;

    public AbstractProfile() {
        counters = new HashMap<String, Long>();
    }

    public Map<String, Long> getCounters() {
        return counters;
    }

    public abstract JSONObject toJSON() throws JSONException;

    protected void populateCounters(JSONObject jo) throws JSONException {
        JSONArray countersObj = new JSONArray();
        for (Map.Entry<String, Long> e : counters.entrySet()) {
            JSONObject jpe = new JSONObject();
            jpe.put("name", e.getKey());
            jpe.put("value", e.getValue());
            countersObj.put(jpe);
        }
        jo.put("counters", countersObj);
    }

    protected void merge(AbstractProfile profile) {
        counters.putAll(profile.counters);
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeInt(counters.size());
        for (Entry<String, Long> entry : counters.entrySet()) {
            output.writeUTF(entry.getKey());
            output.writeLong(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int size = input.readInt();
        counters = new HashMap<String, Long>();
        for (int i = 0; i < size; i++) {
            String key = input.readUTF();
            long value = input.readLong();
            counters.put(key, value);
        }
    }
}