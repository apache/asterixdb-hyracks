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
package edu.uci.ics.hyracks.dataflow.std.group;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractAccumulatingAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[], int[], edu.uci.ics.hyracks.api.comm.IFrameWriter)
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults, IFrameWriter writer)
            throws HyracksDataException {
        return this
                .createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, keyFields, keyFieldsInPartialResults);
    }

    abstract protected IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int[] keyFields,
            final int[] keyFieldsInPartialResults) throws HyracksDataException;

}
