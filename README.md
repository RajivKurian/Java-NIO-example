Java-NIO-example
================

An example  of a multi-threaded NIO server using the Disruptor framework.

To run:
-------

    git clone https://github.com/RajivKurian/Java-NIO-example.git
    sbt run
    Server should start on port 9090.

Details
-------

1.  A single thread accepts connections and  reads byte off the network.
2.  It passes the bytes to another thread via the Disruptor ring buffer. The second thread echoes the writes back to the client.

Features
--------

1.  A naive implementation of a buddy memory allocator written with Java Unsafe. It pools DirectByteBuffers.
2.  An abstraction over a ring buffer that reclaims dynamic resources from a ring  buffer. The ring buffer is tricky to use when it's fields contain dynamically sizes resources for e.g. ByteBuffers since they cannot readily be re-used on cycles. This abstraction detects ring buffer cycles and reclaims all dynamic resources. It also allows an implementation to claim resources in the middle of a cycle in case it runs out.
3.  Using (2) and (1) we create a ByteBuffer pooling ring buffer that hands out ByteBuffers and reclaims them automatically.



