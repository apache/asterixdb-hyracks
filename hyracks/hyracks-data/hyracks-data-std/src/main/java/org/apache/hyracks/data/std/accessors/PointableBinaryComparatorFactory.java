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
package org.apache.hyracks.data.std.accessors;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IComparableForStringWithoutLengthByte;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;

public class PointableBinaryComparatorFactory implements IBinaryComparatorFactory {
    private static final long serialVersionUID = 1L;

    private final IPointableFactory pf;

    public static PointableBinaryComparatorFactory of(IPointableFactory pf) {
        return new PointableBinaryComparatorFactory(pf);
    }

    public PointableBinaryComparatorFactory(IPointableFactory pf) {
        this.pf = pf;
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        final IPointable p = pf.createPointable();
        return new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                if (l1 == 0 && l2 != 0)
                    return -1;
                if (l1 != 0 && l2 == 0)
                    return 1;
                p.set(b1, s1, l1);
                return ((IComparable) p).compareTo(b2, s2, l2);
            }
        };
    }

    // For String Type only. If a string does not include the length (2 byte) in the beginning and
    // the length for a string is provided by the parameter
    public IBinaryComparator createBinaryWithoutLengthByteComparator() {
        final IPointable p = pf.createPointable();
        return new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                if (l1 == 0 && l2 != 0)
                    return -1;
                if (l1 != 0 && l2 == 0)
                    return 1;
                p.set(b1, s1, l1);
                return ((IComparableForStringWithoutLengthByte) p).compareToWithoutLengthByte(b2, s2, l2);
            }
        };
    }

}