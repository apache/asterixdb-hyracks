/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.tests.unit;

import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.RunAndMaxFrameSizePair;
import edu.uci.ics.hyracks.dataflow.std.sort.util.GroupFrameAccessor;
import edu.uci.ics.hyracks.dataflow.std.sort.util.GroupVSizeFrame;
import edu.uci.ics.hyracks.test.support.TestUtils;

public abstract class AbstractRunGeneratorTest {
    static TestUtils testUtils = new TestUtils();
    static ISerializerDeserializer[] SerDers = new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
    static RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);
    static Random GRandom = new Random(System.currentTimeMillis());
    static int[] SortFields = new int[] { 0, 1 };
    static IBinaryComparatorFactory[] ComparatorFactories = new IBinaryComparatorFactory[] {
            PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
            PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) };

    static void assertMaxFrameSizesAreAllEqualsTo(List<RunAndMaxFrameSizePair> maxSize, int pageSize) {
        for (int i = 0; i < maxSize.size(); i++) {
            assertTrue(maxSize.get(i).maxFrameSize == pageSize);
        }
    }

    abstract AbstractSortRunGenerator getSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int numOfInputRecord)
            throws HyracksDataException;

    protected List<RunAndMaxFrameSizePair> testSortRecords(int pageSize, int frameLimit, int numRuns, int minRecordSize,
            int maxRecordSize, HashMap<Integer, String> specialData) throws HyracksDataException {
        IHyracksTaskContext ctx = testUtils.create(pageSize);

        HashMap<Integer, String> keyValuePair = new HashMap<>();
        List<IFrame> frameList = new ArrayList<>();
        prepareData(ctx, frameList, pageSize * frameLimit * numRuns, minRecordSize, maxRecordSize,
                specialData, keyValuePair);
        AbstractSortRunGenerator runGenerator = getSortRunGenerator(ctx, frameLimit, keyValuePair.size());
        runGenerator.open();
        for (IFrame frame : frameList) {
            runGenerator.nextFrame(frame.getBuffer());
        }
        runGenerator.close();
        matchResult(ctx, runGenerator.getRuns(), keyValuePair);
        return runGenerator.getRuns();
    }

    static void matchResult(IHyracksTaskContext ctx, List<RunAndMaxFrameSizePair> runs,
            Map<Integer, String> keyValuePair) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameTupleAccessor fta = new FrameTupleAccessor(RecordDesc);

        HashMap<Integer, String> copyMap = new HashMap<>(keyValuePair);
        assertReadSorted(runs, fta, frame, copyMap);

        HashMap<Integer, String> copyMap2 = new HashMap<>(keyValuePair);
        int maxFrameSizes = 0;
        for (RunAndMaxFrameSizePair run : runs) {
            maxFrameSizes = Math.max(maxFrameSizes, run.maxFrameSize);
        }
        GroupVSizeFrame gframe = new GroupVSizeFrame(ctx, maxFrameSizes);
        GroupFrameAccessor gfta = new GroupFrameAccessor(ctx.getInitialFrameSize(), RecordDesc);
        assertReadSorted(runs, gfta, gframe, copyMap2);
    }

    static int assertFTADataIsSorted(IFrameTupleAccessor fta, Map<Integer, String> keyValuePair, int preKey)
            throws HyracksDataException {

        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);
        for (int i = 0; i < fta.getTupleCount(); i++) {
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 0) + fta.getFieldSlotsLength());
            int key = (int) RecordDesc.getFields()[0].deserialize(di);
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 1) + fta.getFieldSlotsLength());
            String value = (String) RecordDesc.getFields()[1].deserialize(di);

            if (!keyValuePair.get(key).equals(value)) {
                assertTrue(false);
            }
            keyValuePair.remove(key);
            assertTrue(key >= preKey);
            preKey = key;
        }
        return preKey;
    }

    static void assertReadSorted(List<RunAndMaxFrameSizePair> runs, IFrameTupleAccessor fta, IFrame frame,
            Map<Integer, String> keyValuePair) throws HyracksDataException {

        assertTrue(runs.size() > 0);
        for (RunAndMaxFrameSizePair run : runs) {
            run.run.open();
            int preKey = Integer.MIN_VALUE;
            while (run.run.nextFrame(frame)) {
                fta.reset(frame.getBuffer());
                preKey = assertFTADataIsSorted(fta, keyValuePair, preKey);
            }
            run.run.close();
        }
        assertTrue(keyValuePair.isEmpty());
    }

    static void prepareData(IHyracksTaskContext ctx, List<IFrame> frameList, int minDataSize, int minRecordSize,
            int maxRecordSize, Map<Integer, String> specialData, Map<Integer, String> keyValuePair)
            throws HyracksDataException {

        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());
        FrameTupleAppender appender = new FrameTupleAppender();

        int datasize = 0;
        if (specialData != null) {
            for (Map.Entry<Integer, String> entry : specialData.entrySet()) {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, entry.getKey());
                tb.addField(UTF8StringSerializerDeserializer.INSTANCE, entry.getValue());

                VSizeFrame frame = new VSizeFrame(ctx, FrameHelper
                        .calcAlignedFrameSizeToStore(tb.getFieldEndOffsets().length, tb.getSize(), ctx.getInitialFrameSize()));
                appender.reset(frame, true);
                assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
                frameList.add(frame);
                datasize += frame.getFrameSize();
            }
            keyValuePair.putAll(specialData);
        }

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        appender.reset(frame, true);
        while (datasize < minDataSize) {
            tb.reset();
            int key = GRandom.nextInt(minDataSize + 1);
            if (!keyValuePair.containsKey(key)) {
                String value = generateRandomRecord(minRecordSize, maxRecordSize);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, key);
                tb.addField(UTF8StringSerializerDeserializer.INSTANCE, value);

                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    frameList.add(frame);
                    datasize += frame.getFrameSize();
                    frame = new VSizeFrame(ctx, FrameHelper
                            .calcAlignedFrameSizeToStore(tb.getFieldEndOffsets().length, tb.getSize(),
                                    ctx.getInitialFrameSize()));
                    appender.reset(frame, true);
                    assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
                }

                keyValuePair.put(key, value);
            }
        }
        if (appender.getTupleCount() > 0) {
            frameList.add(frame);
        }

    }

    static String generateRandomRecord(int minRecordSize, int maxRecordSize)
            throws HyracksDataException {
        int size = GRandom.nextInt(maxRecordSize - minRecordSize + 1) + minRecordSize;
        return generateRandomFixSizedString(size);

    }

    static String generateRandomFixSizedString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (; size >= 0; --size) {
            char ch = (char) (GRandom.nextInt(26) + 97);
            sb.append(ch);
        }
        return sb.toString();
    }

    static HashMap<Integer, String> generateBigObject(int pageSize, int times) {
        HashMap<Integer, String> map = new HashMap<>(1);
        for (int i = 1; i < times; i++) {
            map.put(GRandom.nextInt(), generateRandomFixSizedString(pageSize * i));
        }
        return map;
    }

    @Test
    public void testAllSmallRecords() throws HyracksDataException {
        int pageSize = 512;
        int frameLimit = 4;
        int numRuns = 2;
        int minRecordSize = pageSize / 8;
        int maxRecordSize = pageSize / 8;
        List<RunAndMaxFrameSizePair> maxSize = testSortRecords(pageSize, frameLimit, numRuns, minRecordSize,
                maxRecordSize, null);
        assertMaxFrameSizesAreAllEqualsTo(maxSize, pageSize);
    }

    @Test
    public void testAllLargeRecords() throws HyracksDataException {
        int pageSize = 2048;
        int frameLimit = 4;
        int numRuns = 2;
        int minRecordSize = pageSize;
        int maxRecordSize = (int) (pageSize * 1.8);
        List<RunAndMaxFrameSizePair> size = testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize,
                null);
        assertMaxFrameSizesAreAllEqualsTo(size, pageSize * 2);
    }

    @Test
    public void testMixedLargeRecords() throws HyracksDataException {
        int pageSize = 128;
        int frameLimit = 4;
        int numRuns = 4;
        int minRecordSize = 20;
        int maxRecordSize = pageSize / 2;
        HashMap<Integer, String> specialPair = generateBigObject(pageSize, frameLimit - 1);
        List<RunAndMaxFrameSizePair> size = testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize,
                specialPair);

        int max = 0;
        for (RunAndMaxFrameSizePair run : size) {
            max = Math.max(max, run.maxFrameSize);
        }
        assertTrue(max == pageSize * (frameLimit - 1));
    }

    @Test(expected = HyracksDataException.class)
    public void testTooBigRecordWillThrowException() throws HyracksDataException {
        int pageSize = 1024;
        int frameLimit = 8;
        int numRuns = 8;
        HashMap<Integer, String> specialPair = generateBigObject(pageSize, frameLimit);
        int minRecordSize = 10;
        int maxRecordSize = pageSize / 2;
        List<RunAndMaxFrameSizePair> size = testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize,
                specialPair);

    }
}
