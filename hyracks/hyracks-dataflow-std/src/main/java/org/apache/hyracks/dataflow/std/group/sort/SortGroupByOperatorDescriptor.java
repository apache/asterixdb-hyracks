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

import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import edu.uci.ics.hyracks.dataflow.std.sort.ISorter;
import edu.uci.ics.hyracks.dataflow.std.sort.RunAndMaxFrameSizePair;

/**
 * This Operator pushes group-by aggregation into the external sort.
 * After the in-memory sort, it aggregates the sorted data before writing it to a run file.
 * During the merge phase, it does an aggregation over sorted results.
 *
 * @author yingyib
 */
public class SortGroupByOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private final int[] groupFields;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final boolean finalStage;
    private Algorithm alg = Algorithm.MERGE_SORT;

    /**
     * @param spec                      , the Hyracks job specification
     * @param framesLimit               , the frame limit for this operator
     * @param sortFields                , the fields to sort
     * @param groupFields               , the fields to group, which can be a prefix subset of sortFields
     * @param firstKeyNormalizerFactory , the normalized key computer factory of the first key
     * @param comparatorFactories       , the comparator factories of sort keys
     * @param partialAggregatorFactory  , for aggregating the input of this operator
     * @param mergeAggregatorFactory    , for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc      , the record descriptor of intermediate data
     * @param outRecordDesc             , the record descriptor of output data
     * @param finalStage                , whether the operator is used for final stage aggregation
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage) {

        super(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, outRecordDesc);
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }

        this.groupFields = groupFields;
        this.mergeAggregatorFactory = mergeAggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outputRecordDesc = outRecordDesc;
        this.finalStage = finalStage;
    }

    /**
     * @param spec                      , the Hyracks job specification
     * @param framesLimit               , the frame limit for this operator
     * @param sortFields                , the fields to sort
     * @param groupFields               , the fields to group, which can be a prefix subset of sortFields
     * @param firstKeyNormalizerFactory , the normalized key computer factory of the first key
     * @param comparatorFactories       , the comparator factories of sort keys
     * @param partialAggregatorFactory  , for aggregating the input of this operator
     * @param mergeAggregatorFactory    , for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc      , the record descriptor of intermediate data
     * @param outRecordDesc             , the record descriptor of output data
     * @param finalStage                , whether the operator is used for final stage aggregation
     * @param alg                       , the in-memory sort algorithm
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage, Algorithm alg) {
        this(spec, framesLimit, sortFields, groupFields, firstKeyNormalizerFactory, comparatorFactories,
                partialAggregatorFactory, mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, finalStage);
        this.alg = alg;
    }

    @Override
    public AbstractSorterOperatorDescriptor.SortActivity getSortActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.SortActivity(id) {
            @Override
            protected AbstractSortRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescriptorProvider) throws HyracksDataException {
                return new ExternalSortGroupByRunGenerator(ctx, sortFields,
                        recordDescriptorProvider.getInputRecordDescriptor(this.getActivityId(), 0), framesLimit,
                        groupFields, firstKeyNormalizerFactory, comparatorFactories, partialAggregatorFactory,
                        partialAggRecordDesc, alg);
            }
        };
    }

    @Override
    public AbstractSorterOperatorDescriptor.MergeActivity getMergeActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.MergeActivity(id) {

            @Override
            protected ExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, IFrameWriter writer, ISorter sorter,
                    List<RunAndMaxFrameSizePair> runs, IBinaryComparator[] comparators,
                    INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortGroupByRunMerger(ctx, sorter, runs, sortFields,
                        recordDescProvider.getInputRecordDescriptor(new ActivityId(odId, SORT_ACTIVITY_ID), 0),
                        partialAggRecordDesc, outputRecordDesc, necessaryFrames, writer, groupFields, nmkComputer,
                        comparators, partialAggregatorFactory, mergeAggregatorFactory, !finalStage);
            }
        };
    }
}