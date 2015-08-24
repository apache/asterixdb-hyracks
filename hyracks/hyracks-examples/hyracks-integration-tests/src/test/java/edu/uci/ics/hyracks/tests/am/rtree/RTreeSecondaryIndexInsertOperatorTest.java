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

package edu.uci.ics.hyracks.tests.am.rtree;

import java.io.DataOutput;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class RTreeSecondaryIndexInsertOperatorTest extends AbstractRTreeOperatorTest {

    public RTreeSecondaryIndexInsertOperatorTest() {
        this.rTreeType = RTreeType.RTREE;
    }

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        createPrimaryIndex();
        loadPrimaryIndex();
        createSecondaryIndex();
        loadSecondaryIndex();
        insertPipeline();
    }

    @Test
    public void searchUpdatedSecondaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build tuple
        ArrayTupleBuilder tb = new ArrayTupleBuilder(secondaryKeyFieldCount);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.2894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.624, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.8894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.024, dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] keyFields = { 0, 1, 2, 3 };

        RTreeSearchOperatorDescriptor secondarySearchOp = new RTreeSearchOperatorDescriptor(spec, secondaryRecDesc,
                storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                secondaryComparatorFactories, keyFields, rtreeDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondarySearchOp, NC1_ID);

        // fifth field from the tuples coming from secondary index
        int[] primaryLowKeyFields = { 4 };
        // fifth field from the tuples coming from secondary index
        int[] primaryHighKeyFields = { 4 };

        // search primary index
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                null, primaryLowKeyFields, primaryHighKeyFields, true, true, btreeDataflowHelperFactory, false, false,
                null, NoOpOperationCallbackFactory.INSTANCE, null, null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primarySearchOp, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondarySearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondarySearchOp, 0, primarySearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Override
    protected IIndexDataflowHelperFactory createDataFlowHelperFactory(
            IPrimitiveValueProviderFactory[] secondaryValueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory,
            int[] btreeFields) throws TreeIndexException {
        return ((RTreeOperatorTestHelper) testHelper).createDataFlowHelperFactory(secondaryValueProviderFactories,
                rtreePolicyType, null, true);
    }

    @Override
    public void cleanup() throws Exception {
        destroyPrimaryIndex();
        destroySecondaryIndex();
    }
}