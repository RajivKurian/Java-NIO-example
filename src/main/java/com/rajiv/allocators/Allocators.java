package com.rajiv.allocators;

// Use as a launch pad for other allocators.
// We could define characteristics like minimize external fragmentation, optimize for large/small buffers etc to
// choose the right allocator.

public class Allocators {
  public static Allocator getNewAllocator(int maxSize, int minSize) {
    return new BinaryBuddy(maxSize, minSize);
  }
}