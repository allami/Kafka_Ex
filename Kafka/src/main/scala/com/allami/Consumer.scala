package com.allami


import java.util.concurrent._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.allami.Config._
import com.allami.Producer.{getTimestamp, producer, topic}

import scala.collection.JavaConversions._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import com.typesafe.scalalogging._
import java.util.regex._

object Consumer  extends App   with LazyLogging{

  val consumer = Config.ConsumerConf.consumer

  consumer.subscribe(Collections.singletonList("data"))

  val records = consumer.poll(1000)

  for (record <- records) {

     logger.info("timestamp :"+ record.timestamp())

      contains(record.value(),"error")
      contains(record.value(),"warn")
      contains(record.value(),"info")

  }

 def  contains(data:String, token:String ) ={
   if (data.contains(token)){
     sendData(data,token)
   }
 }

  def sendData(line:String,topic :String)={

    val regex = "(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}).*".r
    val datetime=line match {
      case regex(date) => Some(date)
      case _ => None
    }
    val timestamp=Producer.getTimestamp(datetime.getOrElse("")).get

    val producer =  Config.ProducerConf.producer
    val  data = new ProducerRecord[String,String](topic,3,timestamp,"ligne", line)

    producer.send(data)

  }


}
