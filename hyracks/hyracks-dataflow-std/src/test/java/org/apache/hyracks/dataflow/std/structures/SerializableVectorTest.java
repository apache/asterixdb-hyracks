package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SerializableVectorTest {
    FrameManager frameManager = new FrameManager(128 * 1024);
    class Int implements IResetableSerializable<Int> {
        int i;
        int j;
        public Int() {
            i = 0;
            j = 0;
        }

        public Int(int i, int j) {
            this.i = i;
            this.j = 1;
        }

        @Override
        public void reset(Int other) {
            i = other.i;
            j = other.j;
        }

        public void serialize(byte[] bytes, int offset){
            writeInt(bytes, offset, i);
            writeInt(bytes, offset + 4, j);
        }

        public void deserialize(byte[] bytes, int offset, int length){
            i = readInt(bytes, offset);
            j = readInt(bytes, offset + 4);
        }

        /**
         * write int value to bytes[offset] ~ bytes[offset+3]
         * @param bytes
         * @param offset
         * @param value
         */
        private void writeInt(byte[] bytes, int offset, int value) {
            int byteIdx = offset;
            bytes[byteIdx++] = (byte) (value >> 24);
            bytes[byteIdx++] = (byte) (value >> 16);
            bytes[byteIdx++] = (byte) (value >> 8);
            bytes[byteIdx] = (byte) (value);
        }

        private int readInt(byte[] bytes, int offset){
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                    + ((bytes[offset + 3] & 0xff) << 0);
        }


    }

    @Test
    public void testInit1(){
        int recordSize = 8;
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    @Test
    public void testInit2(){
        int frameSize = 128 * 1024;
        int recordSize = 8;
        SerializableVector sVector = new SerializableVector(recordSize, frameSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    private void testAppendHelper(int vSize, int recordSize){
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
            assertEquals(i+1, sVector.size());
        }
        int frameSize = frameManager.getInitialFrameSize();
        int frameCount = calculateFrameCount(vSize, frameSize, recordSize);
        assertEquals(frameCount, sVector.getFrameCount());
    }

    @Test
    public void testAppendSmallSize(){
        int recordSize = 8;
        int vSize = 10000;
        testAppendHelper(vSize, recordSize);
    }

    @Test
    public void testAppendLargeSize(){
        int recordSize = 8;
        int vSize = 10000000;    //10M
        testAppendHelper(vSize, recordSize);
    }

    @Test
    public void testAppendLargerSize(){
        int recordSize = 8;
        int vSize = 100000000;  //100M
        testAppendHelper(vSize, recordSize);
    }

    @Test
    public void testAppendNonDefaultFrameSize(){
        int recordSize = 8;
        int frameSize = 10000;
        int vSize = 10000000;    //10M
        SerializableVector sVector = new SerializableVector(recordSize, frameSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
            assertEquals(i+1, sVector.size());
        }
    }
    private void testGetMethodHelper(int frameSize, int recordSize){
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(recordSize, frameSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }

        Int record = new Int();
        for(int i = 0; i < vSize; i ++){
            sVector.get(i, record);
            assertEquals(record, new Int(i, i + 2));
        }
    }

    @Test
    public void testGetMethod1(){
        int frameSize = 128 * 1024;
        int recordSize = 8;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethod2(){
        int frameSize = 100000;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethod3(){
        int frameSize = 32 * 1024;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethodOutOfBound(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }

        Int record = new Int();
        for(int i = vSize; i < vSize + 100; i ++) {
            ret = sVector.get(vSize, record);
            assertFalse(ret);
        }
    }

    @Test
    public void testSetMethod(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }

        Int record = new Int();
        for(int i = 0; i < vSize; i += 5)
            sVector.set(i, new Int(i + 5, i + 6));

        for(int i = 0; i < vSize; i ++){
            sVector.get(i, record);
            if(i % 5 == 0)
                assertEquals(new Int(i + 5, i + 6), record);
            else
                assertEquals(new Int(i, i + 2), record);
        }

    }

    @Test
    public void testSetMethodOutOfBound(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }

        Int record = new Int();
        for(int i = vSize; i < vSize + 100; i ++) {
            ret = sVector.set(vSize, record);
            assertFalse(ret);
        }
    }

    @Test
    public void testClearMethod(){
        int recordSize = 8;
        int vSize = 1000000;    //1M
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        boolean ret;
        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);

        for(int i = 0; i < vSize; i ++){
            ret = sVector.append(new Int(i, i + 2));
            assertTrue(ret);
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);
    }

    private int calculateFrameCount(int vSize, int recordSize, int frameSize){
        return (int)Math.ceil((double)vSize / (frameSize / recordSize));
    }

}