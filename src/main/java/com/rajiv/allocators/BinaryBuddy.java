package com.rajiv.allocators;

import com.rajiv.utils.Utils;
import com.rajiv.platform.*;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;

// A naive implementation of a binary buddy allocator.
// It uses two different ArrayLists to maintain used and unused buffers instead of a single one.
// Deals with ByteBuffers directly instead of having a container class. Contain class might help.

public class BinaryBuddy implements Allocator {
  private static final int DEFAULT_LEVEL_SIZE = 8;
  public int maxCapacity, minCapacity, maxLevel;
  public ArrayList<ArrayDeque<ByteBuffer>> used;
  public ArrayList<ArrayDeque<ByteBuffer>> unused;
  public long startAddress, endAddress;

  private final ByteBuffer[] splitResult = new ByteBuffer[2];

  // maxCapacity and minCapacity in bytes.
  public BinaryBuddy(int maxCapacity, int minCapacity) {
    this.maxCapacity = Utils.nextPowerOfTwo(maxCapacity);
    this.minCapacity = Utils.nextPowerOfTwo(minCapacity);
    int numLevels= Utils.log2(maxCapacity) - Utils.log2(minCapacity) + 1;
    maxLevel = numLevels - 1;
    assert(numLevels > 0);

    used = new ArrayList<ArrayDeque<ByteBuffer>>(numLevels);
    unused = new ArrayList<ArrayDeque<ByteBuffer>>(numLevels);
    for (int i = 0; i < numLevels; i++) {
      used.add(new ArrayDeque<ByteBuffer>(DEFAULT_LEVEL_SIZE));
      unused.add(new ArrayDeque<ByteBuffer>(DEFAULT_LEVEL_SIZE));
    }

    ByteBuffer buffer = ByteBuffer.allocateDirect(maxCapacity);
    addUnusedBufferToLevel(maxLevel, buffer);
    startAddress = PlatformDependent.directBufferAddress(buffer);
    endAddress = startAddress + maxCapacity;
  }

  private void addUnusedBufferToLevel(int level, ByteBuffer buffer) {
    ArrayDeque<ByteBuffer> bufferLevel = unused.get(level);
    bufferLevel.add(buffer);
  }

  private void addUsedBufferToLevel(int level, ByteBuffer buffer) {
    ArrayDeque<ByteBuffer> bufferLevel = used.get(level);
    bufferLevel.add(buffer);
  }

  private int getLevel(int capacity) {
    return Math.max(0,Utils.log2(capacity - minCapacity));
  }

  private void split(ByteBuffer buffer, ByteBuffer[] result) {
    int capacity = buffer.capacity();
    int newCapacity = capacity/2;
    long address1 = PlatformDependent.directBufferAddress(buffer);
    long address2 = address1 + newCapacity;

    neuterBuffer(buffer);

    ByteBuffer buffer1 = ByteBuffer.allocateDirect(0);
    ByteBuffer buffer2 = ByteBuffer.allocateDirect(0);
    PlatformDependent.setDirectBufferAddress(buffer1, address1);
    PlatformDependent.setDirectBufferAddress(buffer2, address2);
    PlatformDependent.setDirectBufferCapacity(buffer1, newCapacity);
    PlatformDependent.setDirectBufferCapacity(buffer2, newCapacity);
    result[0] = buffer1;
    result[1] = buffer2;
  }

  // Render a bytebuffer harmless to GC by setting capacity to 0.
  // TODO: Is this the right way?
  private static void neuterBuffer(ByteBuffer buffer) {
    PlatformDependent.setDirectBufferAddress(buffer, -1);
    PlatformDependent.setDirectBufferCapacity(buffer, 0);
  }

  private int getSufficientLevel(int capacity) {
    int minLevel = getLevel(capacity); // The minimum level we can find a buffer at.
    //System.out.println("Min level where we can find capacity " + capacity + " is " + minLevel);
    int targetLevel = -1;
    while (minLevel <= maxLevel) {
      //System.out.println("MinLevel: " + minLevel);
      ArrayDeque<ByteBuffer> currentLevel = unused.get(minLevel);
      if (currentLevel.size() > 0) {
        targetLevel = minLevel;
        //System.out.println("Found an appropriate level - " + targetLevel);
        break;
      }
      minLevel += 1;
    }
    return targetLevel;
  }

  public ByteBuffer allocate(int cap) {
    ByteBuffer result;
    int capacity =  Utils.nextPowerOfTwo(cap);

    int minLevel = getSufficientLevel(capacity);
    if (minLevel == -1) {
      return null;
    } else {

      ArrayDeque<ByteBuffer> currentLevel = unused.get(minLevel);
      int currentLevelNum = minLevel;
      result = currentLevel.removeLast();

      // Recursively split till we go down to the right size.
      while (result.capacity() > capacity) {
        //System.out.println("Found a buffer with a larger than needed capacity " + result.capacity() + " - splitting.");
        split(result, splitResult);
        result = splitResult[0];
        int lowerLevel = currentLevelNum - 1;
        addUnusedBufferToLevel(lowerLevel, splitResult[1]);
        //System.out.println("Added an unused buffer to level " + lowerLevel);
        //System.out.println("Unused list is now \n" + unused);
        currentLevelNum = lowerLevel;
        currentLevel = unused.get(currentLevelNum);
      }

      // Mark used.
      addUsedBufferToLevel(currentLevelNum, result);
    }
    return result;
  }

  private ByteBuffer removeAdjacent(int levelNum, long address, int capacity) {
    ByteBuffer result = null;
    ArrayDeque<ByteBuffer> level= unused.get(levelNum);
    boolean isBeg = (((startAddress - address) / capacity) % 2 == 0);
    long targetAddress;
    if (isBeg) {
      targetAddress = address + capacity;
    } else {
      targetAddress = address - capacity;
    }

    Iterator<ByteBuffer> it = level.iterator();
    while (it.hasNext()) {
      ByteBuffer current = it.next();
      long currentAddress = PlatformDependent.directBufferAddress(current);
      if (currentAddress == targetAddress) {
        result = current;
        it.remove();
        break;
      }
    }
    return result;
  }

  private static final ByteBuffer merge(ByteBuffer buffer1, ByteBuffer buffer2) {
    ByteBuffer result = buffer1;//ByteBuffer.allocateDirect(0);
    int oldCapacity = PlatformDependent.directBufferCapacity(buffer1);
    int newCapacity = oldCapacity * 2;
    long address1 = PlatformDependent.directBufferAddress(buffer1);
    long address2 = PlatformDependent.directBufferAddress(buffer2);
    long minAddress = Math.min(address1, address2);

    //neuterBuffer(buffer1);
    neuterBuffer(buffer2);

    PlatformDependent.setDirectBufferAddress(result, minAddress);
    PlatformDependent.setDirectBufferCapacity(result, newCapacity);
    return result;
  }

  public void free(ByteBuffer buffer) {
    int capacity = buffer.capacity();
    long address = PlatformDependent.directBufferAddress(buffer);
    long endAddress = address + capacity;

    // This can be prevented with a wrapper object.
    assert(address >= startAddress && endAddress <= this.endAddress);
    assert((this.startAddress - address) % capacity == 0);

    // Remove from used levels.
    int targetLevel = getLevel(capacity);
    ArrayDeque<ByteBuffer> usedLevel = used.get(targetLevel);
    boolean didRemove = usedLevel.remove(buffer);
    assert(didRemove);
    //System.out.println("Found the buffer in a used level removed it. Used is now: " + used +
    //                   "\nUnused is now: " + unused);

    ByteBuffer target = buffer;
    // Recursively merge.
    while (true) {
      //System.out.println("Target level is " + targetLevel);
      ByteBuffer adjacent = removeAdjacent(targetLevel, address, capacity);
      if (adjacent == null) {
        // No buddy found just add it to unusedLevel.
        addUnusedBufferToLevel(targetLevel, target);
        //System.out.println("Did not find a buddy adding it to unused level. Unused is now " + unused);
        break;
      } else {
        // Merge and set address, capacity and next target level.
        target = merge(adjacent, target);
        address = PlatformDependent.directBufferAddress(target);
        capacity = PlatformDependent.directBufferCapacity(target);
        targetLevel += 1;
      }
    }
  }

  public void print() {
    System.out.println("Used buffers: " +used);
    System.out.println("Unused buffers: " + unused);
  }
}