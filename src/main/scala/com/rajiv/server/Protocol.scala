package com.rajiv.server

import java.nio.ByteBuffer

object Protocol {
  val MAX_LENGTH = 1 << 10
}

class Protocol(var length: Int, var payload: ByteBuffer) {

  def reset() {
    length = -1
    payload = null
  }

  def isInitialized(): Boolean = {
    (length > 0 && payload != null)
  }

  def reset(length: Int, payload: ByteBuffer) {
    this.length = length
    this.payload = payload
  }

  def isComplete(): Boolean = {
    payload.position() == length
  }
}

