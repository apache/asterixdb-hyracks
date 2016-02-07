package org.apache.hyracks.dataflow.std.structures;

public class SerializableVector implements ISerializableVector<IResetableComparable>{
    /**
     * constructor
     * default frameSize is the same as system setting
     * @param recordSize
     */
    public SerializableVector(IResetableComparableFactory factory, int recordSize){

    }

    /**
     * constructor
     * @param frameSize
     * @param recordSize
     */
    public SerializableVector(IResetableComparableFactory factory, int frameSize, int recordSize){

    }

    @Override
    public boolean get(int index, IResetableComparable record) {
        return false;
    }

    @Override
    public boolean append(IResetableComparable record) {
        return false;
    }

    @Override
    public boolean set(int index, IResetableComparable record) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int getFrameCount() {
        return 0;
    }
}
