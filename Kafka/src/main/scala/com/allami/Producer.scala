package com.allami

import java.util.{Date, Properties}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Producer extends App {

  val producer =  Config.ProducerConf.producer

  val topic=Config.ProducerConf.topic
  val filename = "/opt/host.log"
  for (line <- Source.fromFile(filename).getLines) {
    val data = new ProducerRecord[String, String](topic, "localhost", line)
    producer.send(data)
    println(line)
  }

  producer.close()



}