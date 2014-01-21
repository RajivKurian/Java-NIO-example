import sbt._
import Keys._

object NIO extends Build {
  import Resolvers._
  import Dependencies._

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    name := "java-nio",
    organization := "experiment",
    version := "1.0",
    scalaVersion := "2.10.3",
    crossPaths := false
  )

  lazy val experiment = Project(
    id = "experiment",
    base = file("."),
    settings = buildSettings ++ 
               Seq(libraryDependencies ++= Dependencies.all, resolvers ++= Resolvers.all)
  )
}

object Resolvers {
    val akka = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
    val sonatype = "Sonatype OSS Release" at "https://oss.sonatype.org/content/repositories/release"

    val all = Seq(akka, sonatype)
}

object Dependencies {
    val akka = Seq("com.typesafe.akka" %% "akka-actor" % "2.2.3",
                   "com.typesafe.akka" %% "akka-remote" % "2.2.3"
                  )

    val trove = Seq("net.sf.trove4j" % "trove4j" % "3.0.2")

    val scalaTest = Seq("org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test")

    val all = akka ++ scalaTest ++ trove
}
