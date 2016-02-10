package org.apache.hyracks.dataflow.std.structures;

public interface IResetableSerializable<T> extends IResetable<T> {
    void serialize(byte[] bytes, int offset);
    void deserialize(byte[] bytes, int offset, int length);
}