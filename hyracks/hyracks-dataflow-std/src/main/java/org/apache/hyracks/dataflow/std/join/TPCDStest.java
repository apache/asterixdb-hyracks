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
package org.apache.hyracks.tests.integration;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import org.junit.Test;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamily;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;

import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;

public class TPCDStest extends AbstractIntegrationTest {

    private static boolean DEBUG = false;

    static RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    static RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer() });
    static RecordDescriptor lineItemDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer()});


    static RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer() });
    static RecordDescriptor lineOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer() });
    static RecordDescriptor custorderLineItemJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer()
    });

    static RecordDescriptor storeSaleDesc=new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer() });
    static RecordDescriptor webSaleDesc=new RecordDescriptor(new ISerializerDeserializer[]  {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
    static RecordDescriptor catalogSaleDesc=new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
    static RecordDescriptor webStoreJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer()

    });
    static RecordDescriptor storeCatalogJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer()

    });

    static RecordDescriptor webStoreCatalogJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer()

    });

    static IValueParserFactory[] custValueParserFactories = new IValueParserFactory[custDesc.getFieldCount()];
    static IValueParserFactory[] orderValueParserFactories = new IValueParserFactory[ordersDesc.getFieldCount()];
    static IValueParserFactory[] lineItemValueParserFactories = new IValueParserFactory[lineItemDesc.getFieldCount()];
    static IValueParserFactory[] storeSaleValueParserFactories = new IValueParserFactory[storeSaleDesc.getFieldCount()];
    static IValueParserFactory[] webSaleValueParserFactories = new IValueParserFactory[webSaleDesc.getFieldCount()];
    static IValueParserFactory[] catalogSaleDescFactories = new IValueParserFactory[catalogSaleDesc.getFieldCount()];



    static {
        Arrays.fill(custValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(orderValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(lineItemValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(storeSaleValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(webSaleValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(catalogSaleDescFactories, UTF8StringParserFactory.INSTANCE);
    }

    private IOperatorDescriptor getPrinter(JobSpecification spec, File file) {
        IFileSplitProvider outputSplitProvider = new ConstantFileSplitProvider(
                new FileSplit[] {
                        new FileSplit(NC1_ID, file.getAbsolutePath()) });

        return DEBUG ? new PlainFileWriterOperatorDescriptor(spec, outputSplitProvider, "|")
                : new NullSinkOperatorDescriptor(spec);
    }
    @Test
    public void customerOrderCIDHybridHashJoin_CaseTPCDs() throws Exception {
        //This is the good order TPC-DS Join by Mingda Li
        //webSales Join storeSales Join catalogSales


        JobSpecification spec = new JobSpecification();
        long startTime = new Date().getTime();
        FileSplit[] catalogSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/catalog_sales4g.tbl"))) };
        IFileSplitProvider catalogSplitsProvider = new ConstantFileSplitProvider(catalogSplits);

        FileSplit[] webSalesSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/web_sales4g.tbl"))) };

        IFileSplitProvider webSalesSplitsProvider = new ConstantFileSplitProvider(webSalesSplits);
        FileSplit[] storeSalesSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/store_sales4g.tbl"))) };
        storeSalesSplits[0].getPartition();
        IFileSplitProvider storeSalesSplitsProvider = new ConstantFileSplitProvider(storeSalesSplits);



        FileScanOperatorDescriptor storeSaleScanner = new FileScanOperatorDescriptor(spec, storeSalesSplitsProvider,
                new DelimitedDataTupleParserFactory(storeSaleValueParserFactories, '|'), storeSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, storeSaleScanner, NC1_ID);

        FileScanOperatorDescriptor webSalescanner = new FileScanOperatorDescriptor(spec, webSalesSplitsProvider,
                new DelimitedDataTupleParserFactory(webSaleValueParserFactories, '|'), webSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, webSalescanner, NC1_ID);

        FileScanOperatorDescriptor catalogSalesScanner = new FileScanOperatorDescriptor(spec, catalogSplitsProvider,
                new DelimitedDataTupleParserFactory(catalogSaleDescFactories, '|'), catalogSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, catalogSalesScanner, NC1_ID);


        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 3 , 4 }, new int[] { 2 , 3 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE, UTF8StringBinaryHashFunctionFamily.INSTANCE  },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY)},
                webStoreJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);



        OptimizedHybridHashJoinOperatorDescriptor join2 = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 3 }, new int[] { 15},
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                webStoreCatalogJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join2, NC1_ID);


        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());


        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor webJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(webJoinConn, webSalescanner, 0, join, 0);

        IConnectorDescriptor storeJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(storeJoinConn, storeSaleScanner, 0, join, 1);

        IConnectorDescriptor webStoreJoinResultConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(webStoreJoinResultConn, join, 0, join2, 0);
        IConnectorDescriptor thirdJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(thirdJoinConn, catalogSalesScanner, 0, join2, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        //System.out.println("output to " + file.getAbsolutePath());
        long endTime = new Date().getTime();
        System.out.println("it run for good" + (endTime - startTime)
                + " 。" );
        File temp=new File("tpcDS2G");
        runTestAndStoreResult(spec, temp);
    }
    @Test
    public void customerOrderCIDHybridHashJoin_CaseTPCDsBad() throws Exception {
        //This is the bad order TPC-DS Join by Mingda Li
        //webSales Join storeSales Join catalogSales


        JobSpecification spec = new JobSpecification();
        long startTime = new Date().getTime();
        FileSplit[] catalogSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/catalog_sales4g.tbl"))) };
        IFileSplitProvider catalogSplitsProvider = new ConstantFileSplitProvider(catalogSplits);

        FileSplit[] webSalesSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/web_sales4g.tbl"))) };

        IFileSplitProvider webSalesSplitsProvider = new ConstantFileSplitProvider(webSalesSplits);
        FileSplit[] storeSalesSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/store_sales4g.tbl"))) };
        storeSalesSplits[0].getPartition();
        IFileSplitProvider storeSalesSplitsProvider = new ConstantFileSplitProvider(storeSalesSplits);



        FileScanOperatorDescriptor storeSaleScanner = new FileScanOperatorDescriptor(spec, storeSalesSplitsProvider,
                new DelimitedDataTupleParserFactory(storeSaleValueParserFactories, '|'), storeSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, storeSaleScanner, NC1_ID);

        FileScanOperatorDescriptor webSalescanner = new FileScanOperatorDescriptor(spec, webSalesSplitsProvider,
                new DelimitedDataTupleParserFactory(webSaleValueParserFactories, '|'), webSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, webSalescanner, NC1_ID);

        FileScanOperatorDescriptor catalogSalesScanner = new FileScanOperatorDescriptor(spec, catalogSplitsProvider,
                new DelimitedDataTupleParserFactory(catalogSaleDescFactories, '|'), catalogSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, catalogSalesScanner, NC1_ID);


        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 200, 243,
                1.2, new int[] { 2 }, new int[] { 15 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY)},
                storeCatalogJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);



        OptimizedHybridHashJoinOperatorDescriptor join2 = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 2, 3 }, new int[] { 3, 4},
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE ,UTF8StringBinaryHashFunctionFamily.INSTANCE},
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                webStoreCatalogJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join2, NC1_ID);


        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());


        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor catalogJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(catalogJoinConn, catalogSalesScanner, 0, join, 1);

        IConnectorDescriptor storeJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(storeJoinConn, storeSaleScanner, 0, join, 0);

        IConnectorDescriptor webStoreJoinResultConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(webStoreJoinResultConn, join, 0, join2, 0);
        IConnectorDescriptor thirdJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(thirdJoinConn, webSalescanner, 0, join2, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        //System.out.println("output to " + file.getAbsolutePath());
        long endTime = new Date().getTime();
        System.out.println("it run for bad" + (endTime - startTime)
                + " 。" );
        File temp=new File("tpcDS8G");
        runTestAndStoreResult(spec, temp);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_CaseTPCDs1() throws Exception {
        //This is the bad order TPC-DS Join by Mingda Li
        //webSales Join storeSales Join catalogSales


        JobSpecification spec = new JobSpecification();
        long startTime = new Date().getTime();
        FileSplit[] catalogSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/catalog_sales.tbl"))) };
        IFileSplitProvider catalogSplitsProvider = new ConstantFileSplitProvider(catalogSplits);

        FileSplit[] storeSalesSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/store_sales.tbl"))) };
        storeSalesSplits[0].getPartition();
        IFileSplitProvider storeSalesSplitsProvider = new ConstantFileSplitProvider(storeSalesSplits);



        FileScanOperatorDescriptor storeSaleScanner = new FileScanOperatorDescriptor(spec, storeSalesSplitsProvider,
                new DelimitedDataTupleParserFactory(storeSaleValueParserFactories, '|'), storeSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, storeSaleScanner, NC1_ID);


        FileScanOperatorDescriptor catalogSalesScanner = new FileScanOperatorDescriptor(spec, catalogSplitsProvider,
                new DelimitedDataTupleParserFactory(catalogSaleDescFactories, '|'), catalogSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, catalogSalesScanner, NC1_ID);






        OptimizedHybridHashJoinOperatorDescriptor join2 = new OptimizedHybridHashJoinOperatorDescriptor(spec, 63, 243,
                1.2, new int[] { 2 }, new int[] { 15 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                storeCatalogJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join2, NC1_ID);


        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());


        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);


        IConnectorDescriptor storeJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(storeJoinConn, storeSaleScanner, 0, join2, 0);


        IConnectorDescriptor thirdJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(thirdJoinConn, catalogSalesScanner, 0, join2, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join2, 1, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        //System.out.println("output to " + file.getAbsolutePath());
        long endTime = new Date().getTime();
        System.out.println("it run for good" + (endTime - startTime)
                + " 。" );
        File temp=new File("tpcDS8G");
        runTestAndStoreResult(spec, temp);
    }
    @Test
    public void customerOrderCIDHybridHashJoin_Case1() throws Exception {
        //This is the good order Join by Mingda Li
        //This is the bad order Join by Mingda Li
        //This is the bad order Join by Mingda Li
        //This is the bad order Join by Mingda Li
        //This is the bad order Join by Mingda Li

        JobSpecification spec = new JobSpecification();
        long startTime = new Date().getTime();
        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customerlmdd.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] lineSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/lineitem1g.tbl"))) };
        IFileSplitProvider lineSplitsProvider = new ConstantFileSplitProvider(lineSplits);

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orderslmdd.tbl"))) };
        ordersSplits[0].getPartition();
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);


        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);



        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        FileScanOperatorDescriptor lineItemScanner = new FileScanOperatorDescriptor(spec, lineSplitsProvider,
                new DelimitedDataTupleParserFactory(lineItemValueParserFactories, '|'), lineItemDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, lineItemScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join2 = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 0 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custorderLineItemJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join2, NC1_ID);


        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

 /*       File file = File.createTempFile(getClass().getName(), "case1");
        IOperatorDescriptor printer = getPrinter(spec, file);*/
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor ordCustJoinResultConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordCustJoinResultConn, join, 0, join2, 1);
        IConnectorDescriptor thirdJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(thirdJoinConn, lineItemScanner, 0, join2, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        //System.out.println("output to " + file.getAbsolutePath());
        long endTime = new Date().getTime();
        System.out.println("it run for bad" + (endTime - startTime)
                + " 。" );
        File temp=new File("result2join");
        runTestAndStoreResult(spec, temp);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case12() throws Exception {
        //This is the good order Join by Mingda Li
        //This is the good order Join by Mingda Li
        //This is the good order Join by Mingda Li
        //This is the good order Join by Mingda Li
        //This is the good order Join by Mingda Li


        JobSpecification spec = new JobSpecification();
        long startTime = new Date().getTime();
        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customerlmdd.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] lineSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/lineitem1g.tbl"))) };
        IFileSplitProvider lineSplitsProvider = new ConstantFileSplitProvider(lineSplits);

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orderslmdd.tbl"))) };
        ordersSplits[0].getPartition();
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);


        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);
        FileScanOperatorDescriptor lineItemScanner = new FileScanOperatorDescriptor(spec, lineSplitsProvider,
                new DelimitedDataTupleParserFactory(lineItemValueParserFactories, '|'), lineItemDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, lineItemScanner, NC1_ID);




        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 0 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                lineOrderJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);


        OptimizedHybridHashJoinOperatorDescriptor join2 = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custorderLineItemJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join2, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case1");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor lineJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(lineJoinConn, lineItemScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor ordCustJoinResultConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordCustJoinResultConn, join, 0, join2, 1);
        IConnectorDescriptor thirdJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(thirdJoinConn, custScanner, 0, join2, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
        long endTime = new Date().getTime();
        System.out.println("it run for good " + (endTime - startTime)
                + " 。" );
        File temp=new File("result2join");
        runTestAndStoreResult(spec, temp);
    }
  /*  @Test
    public void customerOrderCIDHybridHashJoin_CasetestDS() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] storeSplit = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/store_sales.tbl"))) };
        IFileSplitProvider storeSplitsProvider = new ConstantFileSplitProvider(storeSplit);

        FileSplit[] cataSplit = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/catalog_sales.tbl"))) };

        IFileSplitProvider cataSplitsProvider = new ConstantFileSplitProvider(cataSplit);

        FileScanOperatorDescriptor storeScanner = new FileScanOperatorDescriptor(spec, storeSplitsProvider,
                new DelimitedDataTupleParserFactory(storeSaleValueParserFactories, '|'), storeSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, storeScanner, NC1_ID);

        FileScanOperatorDescriptor cataScanner = new FileScanOperatorDescriptor(spec, cataSplitsProvider,
                new DelimitedDataTupleParserFactory(catalogSaleDescFactories, '|'), catalogSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, cataScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 122,
                1.2, new int[] { 2, 10 }, new int[] { 15, 18 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                storeCatalogJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case2");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, storeScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, cataScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
    }
    @Test
    public void customerOrderCIDHybridHashJoin_CasetestDSStoreWeb() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] webSplit = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/web_sales.tbl"))) };
        IFileSplitProvider webSplitsProvider = new ConstantFileSplitProvider(webSplit);

        FileSplit[] storeSplit = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/store_sales.tbl"))) };

        IFileSplitProvider storeSplitsProvider = new ConstantFileSplitProvider(storeSplit);

        FileScanOperatorDescriptor webScanner = new FileScanOperatorDescriptor(spec, webSplitsProvider,
                new DelimitedDataTupleParserFactory(webSaleValueParserFactories, '|'), webSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, webScanner, NC1_ID);

        FileScanOperatorDescriptor storeScanner = new FileScanOperatorDescriptor(spec, storeSplitsProvider,
                new DelimitedDataTupleParserFactory(storeSaleValueParserFactories, '|'), storeSaleDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, storeScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 122,
                1.2, new int[] { 3, 4 }, new int[] { 2 ,3 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                webStoreJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case2");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, webScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, storeScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
    }*/

    @Test
    public void customerOrderCIDHybridHashJoin_Case1_StatsFirst() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customerlmdd.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orderslmdd.tbl"))) };
        ordersSplits[0].getPartition();
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);


        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case1");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case2() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer3.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders4.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 122,
                1.2, new int[] { 0, 3 }, new int[] { 1, 0 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case2");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case3() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer3.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders1.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 6, 122,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0),
                null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        File file = File.createTempFile(getClass().getName(), "case3");
        IOperatorDescriptor printer = getPrinter(spec, file);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + file.getAbsolutePath());
    }

}
