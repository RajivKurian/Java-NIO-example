package com.rajiv.server

import com.rajiv.allocators._
import com.rajiv.rbutils._

import java.net._
import java.nio._
import java.nio.channels._

import com.lmax.disruptor._
import java.util.concurrent.{Executors}

class NetworkEvent {
  var buffer: ByteBuffer = null
  var socketChannel: SocketChannel = null
}

object NetworkEventFactory extends EventFactory[NetworkEvent] {
  def newInstance(): NetworkEvent = return new NetworkEvent
}

// Based on the Disruptor BatchEventProcessor.
// We have a different class in case we need to make selector calls which will require balancing between
// polling the ring buffer and the selector.
class NetworkEventProcessor(ringBufferNetwork: RingBuffer[NetworkEvent],
                            val sequenceBarrier: SequenceBarrier) extends Runnable {
  val sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE)
  private[this] var readBuffer: ByteBuffer = null

  override def run() {
    var nextSequence = sequence.get() + 1L

    while (true) {
      var availableSequence = sequenceBarrier.waitFor(nextSequence)
      if (nextSequence > availableSequence) {
        Thread.`yield`
      }
      while (nextSequence <= availableSequence) {
        handleEvent(ringBufferNetwork.get(nextSequence), nextSequence)
        nextSequence += 1
      }
      sequence.set(availableSequence)
    } 
  }

  def handleEvent(event: NetworkEvent, sequence: Long) {
    System.out.println("Processor thread new event.")
    readBuffer = event.buffer // Should we duplicate to avoid false sharing?
    while (readBuffer.hasRemaining()) {
      event.socketChannel.write(readBuffer)
      //print(readBuffer.get().asInstanceOf[Char])
    }
    if (readBuffer.hasRemaining()) {
      print("We could not write all bytes, we should express interest in OP_WRITE and write in the future")
    }
  }
}

// A ring buffer + memory allocator that allocates from a memory allocator into ring buffer entries.
// It reclaims memory on it's own since it keeps track of the producer index.
class MemoryCollectorRingBuffer(maxSize: Int, minSize: Int, ringBuffer: RingBuffer[NetworkEvent], 
                                sequenceBarrier: SequenceBarrier)
  extends ResourceCollectorRingBuffer[NetworkEvent, ByteBuffer](ringBuffer, sequenceBarrier) {

  private[this] val allocator = Allocators.getNewAllocator(maxSize, minSize)

  def release(buffer: ByteBuffer) {
    assert(buffer != null)
    allocator.free(buffer)
  }

  def convert(event: NetworkEvent): ByteBuffer = {
    event.buffer
  }

  def getBuffer(size: Int): ByteBuffer = {
    if (size > maxSize || size < minSize) {
      null
    } else {
      var buf = allocator.allocate(size)
      if (buf == null) {
        printResource("About to start a recycle event")
        recycle()
        printResource("After a recycle event")
        allocator.allocate(size)
      } else {
        buf
      }
    }
  }

  private def printResource(msg: String) {
    println("--------------------------" + msg)
    printResource()
  }

  def printResource() {
    allocator.print()
  }
}

object NioServer {
  val DEFAULT_BUFFER_SIZE = 128
  val DEFAULT_PORT = 9090
  val DEFAULT_SELECTOR_TIMEOUT = 100L //100 ms.

  val RING_BUFFER_SIZE = 4

  val BUDDY_POOL_MAX_SIZE = 1024  // Bytes.
  val BUDDY_POOL_MIN_SIZE = 2 // Bytes.
}

// A simple NIO server that does the following:
// 1. It accepts connections.
// 2. It reads bytes off the connection. It uses a MemoryCollector to recycle byte buffers.
// 3. When it reads enough bytes to form a request, it sends it over to another thread to do the requisite processing.
// TODO:
// 1. Implement length prefixed protocol.
// 2. Attach a protocol object to a connection.
//    The protocol object will contain the state and the byte buffer used so far.

class NioServer(port: Int) {
  import NioServer._

  val ringBufferNetwork = RingBuffer.createSingleProducer(NetworkEventFactory, RING_BUFFER_SIZE,
                                                          new YieldingWaitStrategy())
  val sequenceBarrierNetwork = ringBufferNetwork.newBarrier()
  val networkEventProcessor: NetworkEventProcessor = new NetworkEventProcessor(ringBufferNetwork,
                                                                               sequenceBarrierNetwork)
  ringBufferNetwork.addGatingSequences(networkEventProcessor.sequence)

  final val executor = Executors.newSingleThreadExecutor()
  executor.submit(networkEventProcessor)

  val collectingRingBuffer = new MemoryCollectorRingBuffer(BUDDY_POOL_MAX_SIZE, BUDDY_POOL_MIN_SIZE, ringBufferNetwork,
                                                           sequenceBarrierNetwork)

  val (selector, serverChannel) = initSelectorAndBind()

  private[this] def initSelectorAndBind(): (Selector, ServerSocketChannel) = {
    val socketSelector = Selector.open();

    val serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);

    val isa = new InetSocketAddress(port);
    serverChannel.socket().bind(isa);
    println("Server bound on port " + port)
    serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
    (socketSelector, serverChannel);
  }

  def start() {
    while (true) {
      val readyChannels = selector.select()
      if (readyChannels != 0) {
        val keyIterator = selector.selectedKeys.iterator
        while (keyIterator.hasNext) {
          val key = keyIterator.next()
          if (key.isAcceptable) {
            accept(key)
          } else if (key.isReadable) {
            read(key)
          } else if (key.isWritable) {
            println("Key is writable but I don't give a damn")
          } else {
            println("Shouldn't be here")
          }
          keyIterator.remove()
        }
      }
    }
  }

  private def accept(key: SelectionKey) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    socketChannel.configureBlocking(false)
    socketChannel.setOption[java.lang.Boolean](StandardSocketOptions.SO_KEEPALIVE, true);
    socketChannel.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, true);
    socketChannel.register(selector, SelectionKey.OP_READ);
    println("Client is connected at" + socketChannel.getRemoteAddress())
  }

  private def read(key: SelectionKey) {
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    // Memory collector will reclaim buffers on its own.
    val readBuffer = collectingRingBuffer.getBuffer(DEFAULT_BUFFER_SIZE)
    assert(readBuffer != null)
    readBuffer.clear()
    var numRead = 0;
    try {
      numRead = socketChannel.read(readBuffer)
    } catch {
      case e: java.io.IOException => {
        key.cancel()
        socketChannel.close()
        println("Forceful shutdown")
      }
    }
    if (numRead == -1) {
      println("Graceful shutdown")
      key.channel.close();
      key.cancel();
    } else {
      println("Read some bytes from the client" + numRead)
      readBuffer.flip()
      val index = collectingRingBuffer.next()
      val event = collectingRingBuffer.get(index)
      event.buffer = readBuffer
      event.socketChannel = socketChannel
      ringBufferNetwork.publish(index)
    }
  }
}