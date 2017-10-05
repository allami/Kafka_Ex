name := "Kafka"

version := "0.1"

scalaVersion := "2.12.3"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.0.0"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"