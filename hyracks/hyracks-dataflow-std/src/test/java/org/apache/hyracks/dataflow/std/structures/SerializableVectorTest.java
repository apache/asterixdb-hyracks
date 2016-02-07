package org.apache.hyracks.dataflow.std.structures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SerializableVectorTest {
    class IntFactory implements IResetableComparableFactory<Int> {
        @Override
        public IResetableComparable<Int> createResetableComparable() {
            return new Int();
        }
    }

    class Int implements IResetableComparable<Int> {
        int i;

        public Int() {
            i = 0;
        }

        public Int(int i) {
            this.i = i;
        }

        @Override
        public void reset(Int other) {
            i = other.i;
        }

        @Override
        public int compareTo(Int o) {
            return Integer.compare(i, o.i);
        }
    }

    public void testInit1(){
        int recordSize = 8;
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    public void testInit2(){
        int frameSize = 128 * 1024;
        int recordSize = 8;
        SerializableVector sVector = new SerializableVector(new IntFactory(), frameSize, recordSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    public void testAppendSmallSize(){
        int recordSize = 8;
        int vSize = 10000;
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
    }
    public void testAppendLargeSize(){
        int recordSize = 8;
        int vSize = 10000000;    //10M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
    }
    public void testAppendLargerSize(){
        int recordSize = 8;
        int vSize = 100000000;  //100M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++) {
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
    }
    public void testAppendNonDefaultFrameSize(){
        int recordSize = 8;
        int frameSize = 10000;
        int vSize = 10000000;    //10M
        SerializableVector sVector = new SerializableVector(new IntFactory(), frameSize, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
    }
    private void testGetHelper(int frameSize, int recordSize){
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), frameSize, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        Int record = new Int(-1);
        for(int i = 0; i < vSize; i ++){
            sVector.get(i, record);
            assertEquals(record, new Int(i));
        }
    }
    public void testGet1(){
        int frameSize = 128 * 1024;
        int recordSize = 8;
        testGetHelper(frameSize, recordSize);
    }

    public void testGet2(){
        int frameSize = 100000;
        int recordSize = 16;
        testGetHelper(frameSize, recordSize);
    }

    public void testGet3(){
        int frameSize = 32 * 1024;
        int recordSize = 16;
        testGetHelper(frameSize, recordSize);
    }
    public void testGetOutOfBound(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        Int record = new Int(-1);
        for(int i = vSize; i < vSize + 100; i ++) {
            ret = sVector.get(vSize, record);
            assertFalse(ret);
        }
    }
    public void testSet(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        Int record = new Int(-1);
        for(int i = 0; i < vSize; i += 5)
            sVector.set(i, new Int(i + 5));
        for(int i = 0; i < vSize; i ++){
            sVector.get(i, record);
            if(i % 5 == 0)
                assertEquals(new Int(i + 5), record);
            else
                assertEquals(new Int(i), record);
        }

    }
    public void testSetOutOfBound(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        Int record = new Int(-1);
        for(int i = vSize; i < vSize + 100; i ++) {
            ret = sVector.set(vSize, record);
            assertFalse(ret);
        }
    }
    public void testClear(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);

        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);
    }

    public void testFrameCount(){
        int recordSize = 8;
        int frameSize = 128 * 1024;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), frameSize, recordSize);
        assertEquals(sVector.getFrameCount(), 0);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        int frameNum = vSize / (frameSize / recordSize);
        assertEquals(frameNum, sVector.getFrameCount());

    }
    public void testSize(){
        int recordSize = 8;
        int frameSize = 128 * 1024;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(new IntFactory(), frameSize, recordSize);
        assertEquals(sVector.size(), 0);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i));
            assertTrue(ret);
        }
        assertEquals(sVector.size(), vSize);
    }
}
