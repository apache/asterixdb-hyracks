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
package org.apache.hyracks.algebricks.data.impl;

import java.io.PrintStream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    public static final UTF8StringPrinterFactory INSTANCE = new UTF8StringPrinterFactory();

    private UTF8StringPrinterFactory() {
    }

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                int strlen = UTF8StringPointable.getUTFLength(b, s);
                int pos = s + 2;
                int maxPos = pos + strlen;
                ps.print("\"");
                while (pos < maxPos) {
                    char c = UTF8StringPointable.charAt(b, pos);
                    switch (c) {
                        case '\\':
                        case '"':
                            ps.print('\\');
                            break;
                    }
                    ps.print(c);
                    pos += UTF8StringPointable.charSize(b, pos);
                }
                ps.print("\"");
            }

            @Override
            public void init() throws AlgebricksException {
            }
        };
    }

}
