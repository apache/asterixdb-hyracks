package org.apache.hyracks.dataflow.std.structures;


import org.apache.hyracks.api.context.IHyracksFrameMgrContext;

import java.util.ArrayList;

public class SerializableVector implements ISerializableVector<IResetableSerializable>{

    ArrayList<byte[]> frames;
    int recordSize;
    int frameSize;
    int recordPerFrame;
    int sVectorSize;
    int frameCount;

    int lastPos;


    private void init(int recordSize, int frameSize){
        this.recordSize = recordSize;
        this.frameSize = frameSize;
        recordPerFrame = frameSize / recordSize;
        sVectorSize = 0;
        frameCount = 0;
        lastPos = 0;
    }
    /**
     * constructor
     * default frameSize is the same as system setting
     * @param recordSize
     */
    public SerializableVector(IHyracksFrameMgrContext ctx, int recordSize){
        frames = new ArrayList<>();
        int frameSize = ctx.getInitialFrameSize();
        init(recordSize, frameSize);
    }

    /**
     * constructor
     * @param frameSize
     * @param recordSize
     */
    public SerializableVector(int recordSize, int frameSize){
        frames = new ArrayList<>();
        init(recordSize, frameSize);
    }

    @Override
    public boolean get(int index, IResetableSerializable record) {
        if(index >= sVectorSize) {
            throw new IndexOutOfBoundsException("index out of bound");
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx), offsetInFrame, recordSize);
        return true;
    }

    @Override
    public boolean append(IResetableSerializable record) {
        if(sVectorSize % recordPerFrame == 0){    //add a new frame
            byte[] frame = new byte[frameSize];
            frameCount ++;
            record.serialize(frame, 0);
            frames.add(frame);
            lastPos = recordSize;
        }
        else{
            int frameIdx = frameCount - 1;
            int offsetInFrame = lastPos;
            record.serialize(frames.get(frameIdx), offsetInFrame);
            lastPos += recordSize;
        }
        return true;
    }

    @Override
    public boolean set(int index, IResetableSerializable record) {
        if(index >= sVectorSize){
            throw new IndexOutOfBoundsException("index out of bound");
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx), offsetInFrame, recordSize);
        return true;
    }

    @Override
    public void clear() {
        frames.clear();
        init(recordSize, frameSize);
    }

    @Override
    public int size() {
        return sVectorSize;
    }

    @Override
    public int getFrameCount() {
        return frameCount;
    }

    private int getFrameIdx(int index){
        return index / recordPerFrame;
    }
    private int getOffsetInFrame(int index){
        return (index % recordPerFrame) * recordSize;
    }
}