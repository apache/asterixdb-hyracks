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


package org.apache.hyracks.dataflow.std.structures;


import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SerializableVector implements ISerializableVector<IResetableSerializable>{

    private ArrayList<ByteBuffer> frames;
    private int recordSize;
    private final int frameSize;
    private final int recordPerFrame;
    private int sVectorSize;
    private int frameCount;

    private int lastPos;

    private IHyracksFrameMgrContext ctx;

    private void init(int recordSize){
        this.recordSize = recordSize;
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
        init(recordSize);
        this.frameSize = frameSize;
        recordPerFrame = frameSize / recordSize;
        this.ctx = ctx;
    }

    /**
     * constructor
     * @param frameSize
     * @param recordSize
     */
    public SerializableVector(int recordSize, int frameSize){
        frames = new ArrayList<>();
        init(recordSize);
        this.frameSize = frameSize;
        recordPerFrame = frameSize / recordSize;
        ctx = new FrameManager(frameSize);
    }

    @Override
    public void get(int index, IResetableSerializable record) {
        if(index >= sVectorSize) {
            throw new IndexOutOfBoundsException("index out of bound");
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx).array(), offsetInFrame, recordSize);
    }

    @Override
    public void append(IResetableSerializable record){
        if(sVectorSize % recordPerFrame == 0){    //add a new frame
            ByteBuffer frame = null;
            try {
                frame = ctx.allocateFrame(frameSize);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            frameCount ++;
            record.serialize(frame.array(), 0);
            frames.add(frame);
            lastPos = recordSize;
        }
        else{
            int frameIdx = frameCount - 1;
            int offsetInFrame = lastPos;
            record.serialize(frames.get(frameIdx).array(), offsetInFrame);
            lastPos += recordSize;
        }
    }

    @Override
    public void set(int index, IResetableSerializable record) {
        if(index >= sVectorSize){
            throw new IndexOutOfBoundsException("index out of bound");
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx).array(), offsetInFrame, recordSize);
    }

    @Override
    public void clear() {
        frames.clear();
        init(recordSize);
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