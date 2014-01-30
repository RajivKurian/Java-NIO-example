package com.rajiv.allocators;

import java.nio.ByteBuffer;

import java.util.ArrayList;


public class FixedCapacityBufferPool {
  final ArrayList<ByteBuffer> list;
  final int capacity;
  public FixedCapacityBufferPool(int capacity, int initialSize) {
    this.capacity = capacity;
    list = new ArrayList<ByteBuffer>(initialSize);
    for (int i = 0; i < list.size(); i++) {
      ByteBuffer buf = list.get(i);
      buf = ByteBuffer.allocateDirect(capacity);
    }
  }

  public ByteBuffer allocate() {
    if (list.isEmpty()) {
      // Allocate a new buffer and use it.
      return ByteBuffer.allocateDirect(capacity);
    } else {
      ByteBuffer buffer = list.remove(list.size() - 1);
      buffer.clear();
      return buffer;
    }
  }
  
  public void free(ByteBuffer buffer) {
    assert(buffer.capacity() == capacity);
    list.add(buffer);
  }
  
  public void print() {
    System.out.println("Plain old pool list is " + list);
  }
}