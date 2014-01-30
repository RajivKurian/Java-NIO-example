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
    println(s"Before writing payload position is ${readBuffer.position()} and limit is ${readBuffer.limit()}")

    while (readBuffer.hasRemaining()) {
      event.socketChannel.write(readBuffer)
      //print(readBuffer.get().asInstanceOf[Char])
    }
    println(s"After writing payload position is ${readBuffer.position()} and limit is ${readBuffer.limit()}")
    println("------------------------------------------------------------------------\n")

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
        buf = allocator.allocate(size)
      } else {
        buf
      }
      buf.clear()
      buf
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

  val LENGTH_BYTES = 4
  val LENGTH_BUFFER_INITIAL_POOL_SIZE = 20
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

  val lengthBufferPool = new FixedCapacityBufferPool(LENGTH_BYTES, LENGTH_BUFFER_INITIAL_POOL_SIZE);

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
          //if (key.isValid())
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
    println("Read event")
    val socketChannel = key.channel().asInstanceOf[SocketChannel]
    val attachment = key.attachment()
    // We check the attachment to see if this is a new client or not.
    attachment match {
      case null => {
        // New client, no old attachment.
        println("No attachment found fresh read")
        readFreshRequest(key, socketChannel, null)
      }
      case buffer: ByteBuffer => {
        println("Attachment is a byte buffer, need to read length")
        // We did not manage to first 4 bytes to read the length of the buffer. So continue reading into it.
        readLength(key, socketChannel, buffer, null)
      }
      case protocol: Protocol => {
        println("Attachment is an instance of protocol")
      // Old client. We might be in the middle of reading a protocol message.
      readProtocol(key, socketChannel, protocol)
      }
    }
  }

  private def readFreshRequest(key: SelectionKey, socketChannel: SocketChannel, existingProtocolObject: Protocol) {
    // First allocate a ByteBuffer of capacity 4 and read into it to figure out the length.
    val lengthBuffer = lengthBufferPool.allocate()
    readLength(key, socketChannel, lengthBuffer, existingProtocolObject)
  }

  private def readLength(key: SelectionKey, socketChannel: SocketChannel, lengthBuffer: ByteBuffer,
                         existingProtocolObject: Protocol) {
    val retVal = readGeneric(key, socketChannel, lengthBuffer)
    if (retVal) {
      if (lengthBuffer.position() < LENGTH_BYTES - 1) {
        println("We could not read 4 bytes of length off the network. Current position is " + lengthBuffer.position())
        // We could not even read the first 4 bytes.
        // Attach the same byte buffer to the selectionKey and try later.
        key.attach(lengthBuffer)
      } else {
        // Read all 4 bytes to figure out the length.
        lengthBuffer.flip()
        //println(s" After flipping LengthBuffer position is ${lengthBuffer.position()} and limit is ${lengthBuffer.limit()}")
        val length = lengthBuffer.getInt()
        // Return lengthBuffer to be reused by the pool.
        lengthBufferPool.free(lengthBuffer)

        // TODO: Disconnect clients where the length is unexpected.
        assert(length > 0 && length < Protocol.MAX_LENGTH)

        // Allocate a buffer and read the rest of the request.
        println("New request has length " + length)
        val readBuffer = collectingRingBuffer.getBuffer(length);
        // TODO: Handle null buffers with back-pressure or using new allocators.
        assert(readBuffer != null)
        var protocol: Protocol = if (existingProtocolObject != null) {
          // Re-use the old protocol object.
          existingProtocolObject.length = length
          existingProtocolObject.payload = readBuffer
          existingProtocolObject
        } else {
          new Protocol(length, readBuffer)
        }

        // Attach the protocol for future reads.
        key.attach(protocol)
        readProtocol(key, socketChannel, protocol)
      }
    } else {
      // Since reading the length of the request was not successful, we closed the socketChannel.
      // We must return the ByteBuffer instance to the pool.
      println("Could not read length of request properly key cancelled.")
      lengthBufferPool.free(lengthBuffer)
    }
  }

  private def readProtocol(key: SelectionKey, socketChannel: SocketChannel, protocol: Protocol) {
    if (protocol.isInitialized) {
      // We are in the middle of reading a new request. Handle it.
      val payload = protocol.payload
      val retval = readGeneric(key, socketChannel, payload)
      if (retval) {
        if (protocol.isComplete) {
          // Publish the event and reset the protocol so future requests work as expected.
          publishMessage(payload, socketChannel)
          protocol.reset()
        } else {
          // Protocol is not complete. Just keep going.
          // No need to attach to the selection key, since we assume that the protocol object
          // has already been attached when the length was read.
          println("Did not finish reading all bytes for this msg, waiting for more bytes in the future")
        }
      } else {
        // Since the request was not successful, we closed the socketChannel.
        // We must return the ByteBuffer instance to the pool.
        println("Could not read request - key cancelled.")
        collectingRingBuffer.release(protocol.payload)
      }
    } else {
      // We reset the old protocol object. We must read the length again.
      println("We are re-using an old protocol object we must read a fresh length")
      readFreshRequest(key, socketChannel, protocol)
    }
  }

  private def publishMessage(payload: ByteBuffer, socketChannel: SocketChannel) {
    println("A complete protocol message has been received. Sending all bytes to consumer thread")
    payload.flip()
    println(s" After flipping payload position is ${payload.position()} and limit is ${payload.limit()}")
    val index = collectingRingBuffer.next()
    val event = collectingRingBuffer.get(index)
    event.buffer = payload
    event.socketChannel = socketChannel
    ringBufferNetwork.publish(index)
  }

    private def readGeneric(key: SelectionKey, socketChannel: SocketChannel, buffer: ByteBuffer): Boolean = {
    var success = true
    var numRead = -1
    try {
      numRead = socketChannel.read(buffer)
      while (numRead > 0) {
        println(s"Generic read: Read $numRead bytes")
        println(s"After read buffer position is ${buffer.position()}")
        numRead = socketChannel.read(buffer)
      }
    } catch {
      case e: java.io.IOException => {
        key.cancel()
        socketChannel.close()
        println("Forceful Shutdown")
        success = false
      }
    }
    if (numRead == -1) {
      println("Graceful shutodwn")
      key.channel.close()
      key.cancel()
      success = false
    }
    success
  }
}