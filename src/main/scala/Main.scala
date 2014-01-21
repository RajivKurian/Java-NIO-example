package com.rajiv;

import com.rajiv.server._

object Main extends Application {
  val server = new NioServer(9090)
  server.start()
}