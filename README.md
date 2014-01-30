Java-NIO-example
================

An example of a multi-threaded NIO server using the Disruptor framework. A length prefixed protocol is used for a simple echo application.

To run:
-------

    git clone https://github.com/RajivKurian/Java-NIO-example.git
    sbt run and start server first. Server should start on port 9090.
    sbt run and start client. Client should connect to port 9090.

Details
-------

1.  A single thread accepts connections and  reads byte off the network.
2.  It passes the bytes to another thread via the Disruptor ring buffer. The second thread echoes the writes back to the client.

Features
--------

1.  A naive implementation of a buddy memory allocator written with Java Unsafe. It pools DirectByteBuffers.
2.  An abstraction over a ring buffer that reclaims dynamic resources from a ring  buffer. The ring buffer is tricky to use when it's fields contain dynamically sized resources for e.g. ByteBuffers since they cannot readily be re-used on cycles. This abstraction detects ring buffer cycles and reclaims all dynamic resources. It also allows an implementation to claim resources in the middle of a cycle in case it runs out.
3.  Using (2) and (1) we create a ByteBuffer pooling ring buffer that hands out ByteBuffers and reclaims them automatically.


More Details
------------

1.  A single thread accepts connections and reads bytes off the wire.
2.  Every new request is prefixed by it's length. The network thread tries to read the first 4 bytes from the wire to figure out the length.
3.  Once it reads the first 4 bytes, it allocates from a binary buddy allocator, a buffer of the right length and reads the rest of the request from it.
4.  A request is considered complete when the expected number of bytes are received. This is cheap to check. No complicated parsing happens on this network thread.
5.  A complete request is sent to a processing thread via the Disruptor Ring Buffer. The client's state is reset and we expect a new length prefixed message.
6.  The processor reads from the ByteBuffer and echos the bytes back. If all bytes cannot be written (uncommon), it copies the buffer and starts it's own selector to figure out when to write.
7.  The processor thread always checks if there are pending requests for a particular client before echoing the bytes back.
8.  The Ring Buffer abstraction automatically collects ByteBuffers from event entries when the processor marks an event processed. This is done on the producer thread and prevents sharing. Hence the processor must copy the bytes if it needs the ByteBuffer beyond the scope of initial processing.

Todo
----------

1.  Since we allocate ByteBuffers optimally based on the length a client specifies, malicious clients can cause us to run out of buffers. We need to use a timer to prevent slowloris attacks. I'll probably use a hashed-wheel timer that gets it's ticks on the network's event loop. We must protect against multiple attacks including:
    1.  A client specifies a large length and then never sends the actual payload. A simple timer plus a malicious client list will help here.
    2.  A client specifies a large length and then sends a few bytes at a time again tying up a buffer and continously resetting the idle timer. We should count the number of timer resets required to finish a request. If it goes above a certain threshold we could disconnect the client and put it on the malicious client list.
2.  The select calls on the processor thread are not done yet. We need to balance between polling the ring buffer and making select calls. We only need to make select calls if all bytes could not be synchronously writen.



