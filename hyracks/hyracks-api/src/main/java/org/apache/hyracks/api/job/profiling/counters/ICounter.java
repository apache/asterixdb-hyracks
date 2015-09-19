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
package org.apache.hyracks.api.job.profiling.counters;

public interface ICounter {
    /**
     * Get the fully-qualified name of the counter.
     * 
     * @return Name of the counter.
     */
    public String getName();

    /**
     * Update the value of the counter to be current + delta.
     * 
     * @param delta
     *            - Amount to change the counter value by.
     * @return the new value after update.
     */
    public long update(long delta);

    /**
     * Set the value of the counter.
     * 
     * @param value
     *            - New value of the counter.
     * @return Old value of the counter.
     */
    public long set(long value);

    /**
     * Get the value of the counter.
     * 
     * @return the value of the counter.
     */
    public long get();
}