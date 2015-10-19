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
package edu.uci.ics.hyracks.dataflow.common.data.normalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class Integer64NormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 8735044913496854551L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            private static final int POSTIVE_LONG_MASK = (3 << 30);
            private static final int NON_NEGATIVE_INT_MASK = (2 << 30);
            private static final int NEGATIVE_LONG_MASK = (0 << 30);

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                long value = LongPointable.getLong(bytes, start);
                int highValue = (int) (value >> 32);
                if (value > Integer.MAX_VALUE) {
                    /**
                     * larger than Integer.MAX
                     */
                    int highNmk = getKey(highValue);
                    highNmk >>= 2;
                    highNmk |= POSTIVE_LONG_MASK;
                    return highNmk;
                } else if (value >=0 && value <= Integer.MAX_VALUE) {
                    /**
                     * smaller than Integer.MAX but >=0
                     */
                    int lowNmk = (int) value;
                    lowNmk >>= 2;
                    lowNmk |= NON_NEGATIVE_INT_MASK;
                    return lowNmk;
                } else {
                    /**
                     * less than 0: have not optimized for that
                     */
                    int highNmk = getKey(highValue);
                    highNmk >>= 2;
                    highNmk |= NEGATIVE_LONG_MASK;
                    return highNmk;
                }
            }

            private int getKey(int value) {
                return value ^ Integer.MIN_VALUE;
            }

        };
    }
}
