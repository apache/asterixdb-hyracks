package org.apache.hyracks.dataflow.std.structures;


public interface ISerializableVector<T> {

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of the element to return
     * @param record the returned record will be to reset
     * @return false if the index is out of range
     *         (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    boolean get(int index, T record);

    /**
     * Appends the specified element to the end of this list.
     *
     */
    boolean append(T record);


    /**
     * Replaces the element at the specified position in this list with the
     * specified element.
     */
    boolean set(int index, T record);


    void clear();

    /**
     * Returns the number of elements in this list.  If this list contains
     * more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of elements in this list
     */
    int size();

    int getFrameCount();
}