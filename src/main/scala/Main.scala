package com.rajiv;

import com.rajiv.client._
import com.rajiv.server._

object Demo {
  val DEFAULT_PORT = 9090
}
object Server extends Application {
  val server = new NioServer(Demo.DEFAULT_PORT)
  server.start()
}

object Client extends Application {
  val client = new Client(Demo.DEFAULT_PORT)
  client.start()
}