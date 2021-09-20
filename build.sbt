import Dependencies._

name := "Luxoft"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies ++= List(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % "test"
)