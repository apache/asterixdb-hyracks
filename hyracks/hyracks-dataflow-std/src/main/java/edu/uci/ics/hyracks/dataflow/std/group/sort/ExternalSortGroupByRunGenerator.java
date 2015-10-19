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
package edu.uci.ics.hyracks.dataflow.std.group.sort;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.EnumFreeSlotPolicy;

/**
 * Group-by aggregation is pushed before run file generation.
 *
 * @author yingyib
 */
public class ExternalSortGroupByRunGenerator extends ExternalSortRunGenerator {

    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;

    public ExternalSortGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc, Algorithm alg) throws HyracksDataException {
        this(ctx, sortFields, inputRecordDesc, framesLimit, groupFields, firstKeyNormalizerFactory, comparatorFactories,
                aggregatorFactory, outRecordDesc, alg, EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc, Algorithm alg, EnumFreeSlotPolicy policy) throws HyracksDataException {
        super(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, inputRecordDesc, alg, policy,
                framesLimit);

        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = inputRecordDesc;
        this.outRecordDesc = outRecordDesc;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                ExternalSortGroupByRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    @Override
    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        //create group-by comparators
        IBinaryComparator[] comparators = new IBinaryComparator[Math
                .min(groupFields.length, comparatorFactories.length)];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        return new PreclusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory,
                this.inRecordDesc, this.outRecordDesc, writer, true);
    }
}