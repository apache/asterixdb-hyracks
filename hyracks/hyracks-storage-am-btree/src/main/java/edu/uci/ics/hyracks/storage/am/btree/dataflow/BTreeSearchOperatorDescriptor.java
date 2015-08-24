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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.NoOpLocalResourceFactoryProvider;

public class BTreeSearchOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    protected final int[] lowKeyFields;
    protected final int[] highKeyFields;
    protected final boolean lowKeyInclusive;
    protected final boolean highKeyInclusive;
    private final int[] minFilterFieldIndexes;
    private final int[] maxFilterFieldIndexes;
    protected boolean useOpercationCallbackProceedReturnResult;
    protected byte[] valuesForUseOperationCallbackProceedReturnResult;

    public BTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory dataflowHelperFactory, boolean retainInput, boolean retainNull,
            INullWriterFactory nullWriterFactory, ISearchOperationCallbackFactory searchOpCallbackProvider,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes) {
        this(spec, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, lowKeyFields, highKeyFields, lowKeyInclusive,
                highKeyInclusive, dataflowHelperFactory, retainInput, retainNull, nullWriterFactory,
                searchOpCallbackProvider, minFilterFieldIndexes, maxFilterFieldIndexes, false, null);
    }

    public BTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory dataflowHelperFactory, boolean retainInput, boolean retainNull,
            INullWriterFactory nullWriterFactory, ISearchOperationCallbackFactory searchOpCallbackProvider,
            int[] minFilterFieldIndexes, int[] maxFilterFieldIndexes, boolean useOpercationCallbackProceedReturnResult,
            byte[] valuesForUseOperationCallbackProceedReturnResult) {
        super(spec, 1, 1, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, dataflowHelperFactory, null, retainInput, retainNull,
                nullWriterFactory, NoOpLocalResourceFactoryProvider.INSTANCE, searchOpCallbackProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        this.lowKeyFields = lowKeyFields;
        this.highKeyFields = highKeyFields;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.minFilterFieldIndexes = minFilterFieldIndexes;
        this.maxFilterFieldIndexes = maxFilterFieldIndexes;
        this.useOpercationCallbackProceedReturnResult = useOpercationCallbackProceedReturnResult;
        this.valuesForUseOperationCallbackProceedReturnResult = valuesForUseOperationCallbackProceedReturnResult;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new BTreeSearchOperatorNodePushable(this, ctx, partition, recordDescProvider, lowKeyFields,
                highKeyFields, lowKeyInclusive, highKeyInclusive, minFilterFieldIndexes, maxFilterFieldIndexes);
    }

    @Override
    public boolean getUseOpercationCallbackProceedReturnResult() {
        return useOpercationCallbackProceedReturnResult;
    }

    @Override
    public byte[] getValuesForOpercationCallbackProceedReturnResult() {
        return valuesForUseOperationCallbackProceedReturnResult;
    }

}
