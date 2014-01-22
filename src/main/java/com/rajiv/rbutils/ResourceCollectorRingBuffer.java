package com.rajiv.rbutils;

import com.lmax.disruptor.*;

// A class that manages dynamic resources (e.g. buffers) belonging to a ring buffer.
// It manages auto-reclamation of resources leaving the allocation to an extension. We reclaim on two events:
//  1. If an entire cycle of the ring buffer has completed- We need to reclaim resources on a cycle, because ring-buffer
//     entries will be over-written otherwise and we will leak.
//  2. If we run out of resources while allocating - On such occasions, the consumer has possibly finished processing
//     certain events. We find such events and reclaim the resources.

public abstract class ResourceCollectorRingBuffer<E, R> {
  private RingBuffer<E> ringBuffer;
  private SequenceBarrier sequenceBarrier;

  private long lastCycledCursor;
  private long consumerCursor;
  private long producerCursor;

  private int ringBufferSize;

  // Let the implementation decide how to reclaim a resource.
  public abstract void release(R resource);

  // Get a resource from a ring buffer entry. Ring buffer entry presumably contains a resource entry.
  public abstract R convert(E entry);

  // Generic print capability.
  public abstract void printResource();

  public ResourceCollectorRingBuffer (RingBuffer<E> ringBuffer, SequenceBarrier sequenceBarrier) {
    this.ringBuffer = ringBuffer;
    this.sequenceBarrier = sequenceBarrier;
    lastCycledCursor = 0L;
    producerCursor = consumerCursor = Sequencer.INITIAL_CURSOR_VALUE;
    this.ringBufferSize = ringBuffer.getBufferSize();
  }

  // Ring buffer functions that are needed by the producer.
  public long next() {
    long index = ringBuffer.next();
    updateProducerCursor(index);
    return index;
  }

  public E get(long index) {
    return ringBuffer.get(index);
  }

  public void publish(long index) {
    ringBuffer.publish(index);
  }


  // Called by the producer every time it gets an index from the ring buffer.
  public void updateProducerCursor(long value) {
    producerCursor = value;
    // On a complete cycle, recycle all entries.
    if (producerCursor - lastCycledCursor == ringBufferSize) {
      System.out.println("Going to recycle all resources lastCycledCursor is " + lastCycledCursor +
                         " producer cursor is " + producerCursor);
      recycleAllEntries();
      lastCycledCursor = producerCursor;
      consumerCursor = producerCursor;
      print("After recycling all entries");
    }
  }

  private void recycleAllEntries() {
    long i = 0L;
    print("Before recyling all. Num events published " + producerCursor);
    while (i <= ringBufferSize - 1) {
      E entry = ringBuffer.get(i);
      R resource = convert(entry);
      release(resource);
      i += 1;
    }
  }

  // Helper method that allows an implementation to reclaim resources in the middle of a ring-buffer cycle.
  // Typical use case should be:
  // 1. Try to allocate resource.
  // 2. If present return else call recycle and try to allocate again.
  protected void recycle() {
    long currentConsumerCursor = sequenceBarrier.getCursor();
    if (currentConsumerCursor - consumerCursor <= ringBufferSize - 1) {
      while (consumerCursor != currentConsumerCursor) {
        consumerCursor += 1;
        print("Releasing resource on slot " + consumerCursor);
        release(convert(ringBuffer.get(consumerCursor)));
      }
      // Update the cycle cursor so that we detect cycles appropriately.
      lastCycledCursor = consumerCursor;
    }
  }

  private void print(String msg) {
    System.out.println("----------------------------------\n" + msg);
    printResource();
  }
}