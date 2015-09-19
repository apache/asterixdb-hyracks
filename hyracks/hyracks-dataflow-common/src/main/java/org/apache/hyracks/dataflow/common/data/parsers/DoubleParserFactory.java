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
package org.apache.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DoubleParserFactory implements IValueParserFactory {
    public static final IValueParserFactory INSTANCE = new DoubleParserFactory();

    private static final long serialVersionUID = 1L;

    private DoubleParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                String s = String.valueOf(buffer, start, length);
                try {
                    out.writeDouble(Double.parseDouble(s));
                } catch (NumberFormatException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}