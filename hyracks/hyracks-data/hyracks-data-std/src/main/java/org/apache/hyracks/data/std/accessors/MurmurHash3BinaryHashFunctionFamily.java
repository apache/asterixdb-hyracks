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
package edu.uci.ics.hyracks.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

/**
 * An implementation of the Murmur3 hash family. The code is implemented based
 * on the original <a
 * href=http://code.google.com/p/guava-libraries/source/browse
 * /guava/src/com/google/common/hash/Murmur3_32HashFunction.java>guava
 * implementation</a> from Google Guava library.
 */
public class MurmurHash3BinaryHashFunctionFamily implements IBinaryHashFunctionFamily {

    public static final IBinaryHashFunctionFamily INSTANCE = new MurmurHash3BinaryHashFunctionFamily();

    private static final long serialVersionUID = 1L;

    private MurmurHash3BinaryHashFunctionFamily() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction(final int seed) {
        return new IBinaryHashFunction() {
            @Override
            public int hash(byte[] bytes, int offset, int length) {
                int h = MurmurHash3BinaryHash.hash(bytes, offset, length, seed);
                return h;
            }
        };
    }
}