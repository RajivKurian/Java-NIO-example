package com.rajiv.allocators;

import java.nio.ByteBuffer;

public interface Allocator {
  ByteBuffer allocate(int size);
  void free(ByteBuffer b);
  void print();
}