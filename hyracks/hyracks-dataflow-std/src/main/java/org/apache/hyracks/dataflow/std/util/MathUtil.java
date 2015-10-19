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

package org.apache.hyracks.dataflow.std.util;

public class MathUtil {
    /**
     * Fast way to calculate the log2(x). Note: x should be >= 1.
     *
     * @param n
     * @return
     */
    public static int log2Floor(int n) {
        assert n >= 1;
        int log = 0;
        if (n > 0xffff) {
            n >>>= 16;
            log = 16;
        }

        if (n > 0xff) {
            n >>>= 8;
            log |= 8;
        }

        if (n > 0xf) {
            n >>>= 4;
            log |= 4;
        }

        if (n > 0b11) {
            n >>>= 2;
            log |= 2;
        }

        return log + (n >>> 1);
    }
}
