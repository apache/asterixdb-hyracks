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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputSourcePushRuntime;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class EmptyTupleSourceRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    public EmptyTupleSourceRuntimeFactory() {
    }

    @Override
    public String toString() {
        return "ets";
    }

    @Override
    public IPushRuntime createPushRuntime(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new AbstractOneInputSourcePushRuntime() {

            private ArrayTupleBuilder tb = new ArrayTupleBuilder(0);
            private FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
                appender.flush(writer, true);
                writer.close();
            }
        };
    }

}
