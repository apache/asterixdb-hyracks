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
package org.apache.hyracks.algebricks.common.utils;

import java.io.Serializable;

public class Pair<T1, T2> implements Serializable {

    private static final long serialVersionUID = 1L;
    public T1 first;
    public T2 second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair<?, ?>)) {
            return false;
        } else {
            Pair<?, ?> p = (Pair<?, ?>) obj;
            return this.first.equals(p.first) && this.second.equals(p.second);
        }
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 31 + second.hashCode();
    }

}