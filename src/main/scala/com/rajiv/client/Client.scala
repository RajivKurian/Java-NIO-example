package com.rajiv.client

import java.io._
import java.net._


class Client (portNumber: Int){
  val hostName = "localhost"

  def start() {
    try {
      val socket = new Socket(hostName, portNumber)
      val out = new DataOutputStream(socket.getOutputStream())
      val in = new DataInputStream(socket.getInputStream())
      val stdIn = new BufferedReader(new InputStreamReader(System.in))
      var userInput: String = null
      userInput = stdIn.readLine()
      while (userInput != null) {
        val bytes = userInput.getBytes("UTF-8")
        val length = bytes.length
        println("Sending string " + userInput + " of length " + length)
        out.writeInt(length)
        out.write(bytes, 0, bytes.length)
        var count = 0
        println("Echo: ")
        while (count != length) {
          print(in.readByte().asInstanceOf[Char])
          count += 1
        }
        println("\nDone echoing\n")
        userInput = stdIn.readLine()
        println("User input is " + userInput)
      }
    } catch {
      case e: UnknownHostException => println("Don't know about host" + hostName); System.exit(1)
      case e: IOException => println("Couldn't get I/O for connection to " + hostName); System.exit(1)
      case e: Exception => println("Unknown exception " + e)
    }
  }
}