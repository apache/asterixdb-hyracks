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
package org.apache.hyracks.algebricks.examples.piglet.compiler;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.algebricks.data.impl.IntegerPrinterFactory;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;
import org.apache.hyracks.algebricks.examples.piglet.types.Type;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;

public class PigletPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final PigletPrinterFactoryProvider INSTANCE = new PigletPrinterFactoryProvider();

    private PigletPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {
        Type t = (Type) type;
        switch (t.getTag()) {
            case INTEGER:
                return IntegerPrinterFactory.INSTANCE;
            case CHAR_ARRAY:
                return CharArrayPrinterFactory.INSTANCE;
            case FLOAT:
                return FloatPrinterFactory.INSTANCE;
            default:
                throw new UnsupportedOperationException();

        }
    }

    public static class CharArrayPrinterFactory implements IPrinterFactory {

        private static final long serialVersionUID = 1L;

        public static final CharArrayPrinterFactory INSTANCE = new CharArrayPrinterFactory();

        private CharArrayPrinterFactory() {
        }

        @Override
        public IPrinter createPrinter() {
            return new IPrinter() {
                @Override
                public void init() throws AlgebricksException {
                }

                @Override
                public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                    try {
                        WriteValueTools.writeUTF8String(b, s, l, ps);
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                }
            };
        }
    }

    public static class FloatPrinterFactory implements IPrinterFactory {

        private static final long serialVersionUID = 1L;

        public static final FloatPrinterFactory INSTANCE = new FloatPrinterFactory();

        private FloatPrinterFactory() {
        }

        @Override
        public IPrinter createPrinter() {
            return new IPrinter() {
                @Override
                public void init() throws AlgebricksException {
                }

                @Override
                public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                    ps.print(FloatPointable.getFloat(b, s));
                }
            };
        }
    }

}
