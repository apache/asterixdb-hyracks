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
package edu.uci.ics.hyracks.data.std.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The most simple implementation of a static hashtable you could imagine.
 * Intended to work with binary data and be able to map arbitrary key types to
 * arbitrary value types, given that they have implementations of
 * IBinaryHashFunction and IBinaryComparator.
 * each key in the hash table: the offset (2 byte), length of an entry (2 byte). Additionally, it has the count (1 byte) in a byte array.
 * Hash value: based on an entry value, it will be calculated.
 * This class is NOT thread safe. - For single thread access only
 * Limitation -
 * 		A frame size can't be greater than 64K
 * 		Can't have more than 64K frames.
 *
 * Copied and modified BinaryHashMap by Taewoo
 */
public class BinaryHashSet {
    // Special value to indicate an empty "bucket" in the header array.
    private static final int NULL_PTR = -1;
    private static final int PTR_SIZE = 4;  // 2 byte - frameIdx, 2 byte - frameOffset
    private static final int SLOT_SIZE = 2;

    // This hash-set also stores the count of the real key.
    // It's not part of the key and can be used to indicate whether this key exists in a different array or not.
    private static final int COUNT_SIZE = 1;
    private static final int ENTRY_HEADER_SIZE = 2 * SLOT_SIZE + PTR_SIZE + COUNT_SIZE;
    // We are using 2 byte. Therefore, the limit is 64K.
    private static final int NO_OF_FRAME_LIMIT = 65535;
    private static final int ONE_FRAME_SIZE_LIMIT = 65535;
    private final IBinaryHashFunction hashFunc;
    private final IBinaryComparator cmp;

    private final int[] listHeads;
    private final int frameSize;
    private final List<ByteBuffer> frames = new ArrayList<ByteBuffer>();
    private int currFrameIndex;
    private int nextOff;
    private int size;

    // Byte array that holds the real data for this hashset
    private byte[] refArray;

    // A class that stores a meta-data (offset, length) of the real key in the refArray.
    public static class BinaryEntry {
        public int off;
        public int len;

        public void set(int offset, int length) {
            this.off = offset;
            this.len = length;
        }
    }

    // Initialize a hash-set. It will contain one frame by default.
    public BinaryHashSet(int tableSize, int frameSize, IBinaryHashFunction hashFunc, IBinaryComparator cmp, byte[] refArray) {
        listHeads = new int[tableSize];
        if (frameSize > ONE_FRAME_SIZE_LIMIT) {
            throw new IllegalStateException("A frame size can't be greater than " + ONE_FRAME_SIZE_LIMIT + ". Can't continue.");
        }
        this.frameSize = frameSize;
        this.hashFunc = hashFunc;
        this.cmp = cmp;
        frames.add(ByteBuffer.allocate(frameSize));
        clear();
        this.refArray = refArray;
    }

    /**
     * Set the byte array that the keys in this hash-set refer to.
     *
     * @param refArray
     */
    public void setRefArray(byte[] refArray) {
        this.refArray = refArray;
    }

    /**
     * Inserts a key (off, len) into the hash table.
     * The count of the key will not be changed.
     *
     * @param key
     * @return the current count of the key: when a given key is inserted or that key is already there.
     *         -1: when an insertion fails.
     * @throws HyracksDataException
     */
    public int put(BinaryEntry key) throws HyracksDataException {
        return putFindInternal(key, true, null);
    }

    /**
     * Find whether the given key from an array exists in the hash table.
     * If the key exists, then the count will be increased by 1.
     * This method returns true if the key exists. If not, returns false.
     *
     * @param key
     * @param keyArray
     * @return the current count of the key: when a given key exists.
     *         -1: when the given key doesn't exist.
     *
     * @throws HyracksDataException
     */
    public int find(BinaryEntry key, byte[] keyArray) throws HyracksDataException {
        return putFindInternal(key, false, keyArray);
    }

    // Put an entry or find an entry
    private int putFindInternal(BinaryEntry key, boolean put, byte[] keyArray) throws HyracksDataException {
        int bucket = 0;
        if (put) {
            bucket = Math.abs(hashFunc.hash(this.refArray, key.off, key.len) % listHeads.length);
        } else {
        	// find case
            bucket = Math.abs(hashFunc.hash(keyArray, key.off, key.len) % listHeads.length);
        }
        int headPtr = listHeads[bucket];
        if (headPtr == NULL_PTR) {
            // Key definitely doesn't exist yet.
            if (put) {
                // Key is being inserted.
                listHeads[bucket] = appendEntry(key);
                return 0;
            } else {
            	// find case - the bucket is empty: return false since there is no element in the hash-set
            	return -1;
            }

        }
        // if headPtr is not null,
        // follow the chain in the bucket until we found an entry matching the given key.
        int frameNum = 0;
        int frameOff = 0;
        int entryKeyOff = 0;
        int entryKeyLen = 0;
        int entryCount = 0;
        ByteBuffer frame;
        do {
        	// Get frame num and frame offset from the ptr
            frameNum = getFrameIndex(headPtr);
            frameOff = getFrameOffset(headPtr);
            frame = frames.get(frameNum);

            // Get entry offset
            entryKeyOff = (int) frame.getChar(frameOff);
            entryKeyLen = (int) frame.getChar(frameOff + SLOT_SIZE);

            if (put) {
            	// first check the key length. If they don't match, we don't even need to compare two entries.
                if (entryKeyLen == key.len &&
                	cmp.compare(this.refArray, entryKeyOff, entryKeyLen, this.refArray, key.off, key.len) == 0) {
                    // put - Key found, return true since we return true when the key is already in the hash-map.
                    entryCount = (int) frame.get(frameOff + SLOT_SIZE * 2);
                    return entryCount;
                }
            } else {
            	// find case
            	// first check the key length. If they don't match, we don't even need to compare two entries.
                if (entryKeyLen == key.len &&
                	cmp.compare(this.refArray, entryKeyOff, entryKeyLen, keyArray, key.off, key.len) == 0) {
                    // find - Key found, increase the count and return the count. The maximum count is 255.
                    entryCount = (int) frame.get(frameOff + SLOT_SIZE * 2);
                	if (entryCount < 255) {
                		entryCount++;
                	}
                	frame.put(frameOff + 2 * SLOT_SIZE, (byte) entryCount);
                    return entryCount;
                }
            }
            // Get next key position
            headPtr = frame.getInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE);
        } while (headPtr != NULL_PTR);

        // We've followed the chain to its end, and didn't find the key.
        if (put) {
            // Append the new entry, and set a pointer to it in the last entry we've checked.
        	// put case - success
            int newPtr = appendEntry(key);
            frame.putInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE, newPtr);
            return 0;
        } else {
        	// find case - fail
            return -1;
        }
    }

    public int appendEntry(BinaryEntry key) {
        ByteBuffer frame = frames.get(currFrameIndex);
        int requiredSpace = ENTRY_HEADER_SIZE;
        if (nextOff + requiredSpace >= frameSize) {
            // Entry doesn't fit on the current frame, allocate a new one.
            if (requiredSpace > frameSize) {
                throw new IllegalStateException("A hash key is greater than the framesize: " + frameSize + ". Can't continue.");
            } else if (frames.size() > NO_OF_FRAME_LIMIT) {
                throw new IllegalStateException("There can't be more than " + NO_OF_FRAME_LIMIT + "frames. Can't continue.");
            }
            frames.add(ByteBuffer.allocate(frameSize));
            currFrameIndex++;
            nextOff = 0;
            frame = frames.get(currFrameIndex);
        }
        writeEntryHeader(frame, nextOff, key.off, key.len, 0, NULL_PTR);
        int entryPtr = getEntryPtr(currFrameIndex, nextOff);
        nextOff += requiredSpace;
        size++;
        return entryPtr;
    }

    // [2 byte for key offset] [2 byte for key length] [1 byte for key count] [2 byte for the frame num] [2 byte for the frame offset]
    private void writeEntryHeader(ByteBuffer frame, int targetOff, int keyOff, int keyLen, int keyCount, int ptr) {
        // key offset
    	frame.putChar(targetOff, (char) keyOff);
    	// key length
        frame.putChar(targetOff + SLOT_SIZE, (char) keyLen);
    	// key count
        frame.put(targetOff + SLOT_SIZE * 2, (byte) keyCount);
        // frame no [high 2 byte] frame offset [low 2 byte]
        frame.putInt(targetOff + 2 * SLOT_SIZE + COUNT_SIZE, ptr);
    }

    private int getEntryPtr(int frameIndex, int frameOff) {
        return (frameIndex << 16) + frameOff;
    }

    private int getFrameIndex(int ptr) {
        return (int) ((ptr >> 16) & 0xffff);
    }

    private int getFrameOffset(int ptr) {
        return (int) (ptr & 0xffff);
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size > 0;
    }

    public void clear() {
        // Initialize all entries to point to nothing.
        Arrays.fill(listHeads, NULL_PTR);
        currFrameIndex = 0;
        nextOff = 0;
        size = 0;
        this.refArray = null;
    }

    public Iterator<BinaryEntry> iterator() {
        return new BinaryHashSetIterator();
    }

    public class BinaryHashSetIterator implements Iterator<BinaryEntry> {
        private final BinaryEntry val = new BinaryEntry();
        private int listHeadIndex;
        private ByteBuffer frame;
        private int frameIndex;
        private int frameOff;

        public BinaryHashSetIterator() {
            listHeadIndex = 0;
            frame = null;
            frameIndex = -1;
            frameOff = -1;
        }

        @Override
        public boolean hasNext() {
        	return hasNextElement(false);
        }

        // When we need to clear the count of each key, clearCount is set to true.
        // Otherwise, set to false.
        public boolean hasNextElement(boolean clearCount) {
            if (frame != null) {
                int nextPtr = frame.getInt(frameOff + 2 * SLOT_SIZE + COUNT_SIZE);
                if (nextPtr == NULL_PTR) {
                    // End of current list.
                    listHeadIndex++;
                    return nextListHead(clearCount);
                } else {
                    // Follow pointer.
                	if (!clearCount) {
                        setValue(nextPtr);
                	} else {
                		clearCount(nextPtr);
                	}
                    return true;
                }
            }
            return nextListHead(clearCount);
        }

        private boolean nextListHead(boolean clearCount) {
            // Position to first non-null list-head pointer.
            while (listHeadIndex < listHeads.length && listHeads[listHeadIndex] == NULL_PTR) {
                listHeadIndex++;
            }
            if (listHeadIndex < listHeads.length) {
                // Positioned to first non-null list head.
            	if (!clearCount) {
                    setValue(listHeads[listHeadIndex]);
            	} else {
            		clearCount(listHeads[listHeadIndex]);
            	}
                return true;
            } else {
                // No more lists.
                frame = null;
                return false;
            }
        }

        private void setValue(int ptr) {
            frameIndex = getFrameIndex(ptr);
            frameOff = getFrameOffset(ptr);
            frame = frames.get(frameIndex);
            int entryKeyOff = (int) frame.getChar(frameOff);
            int entryKeyLen = (int) frame.getChar(frameOff + SLOT_SIZE);
            val.set(entryKeyOff, entryKeyLen);
        }

        // Clear the count of a given key
        private void clearCount(int ptr) {
            frameIndex = getFrameIndex(ptr);
            frameOff = getFrameOffset(ptr);
            frame = frames.get(frameIndex);
            frame.put(frameOff + 2 * SLOT_SIZE, (byte) 0);
        }

        @Override
        public BinaryEntry next() {
            return val;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not implemented");
        }
    }
}
