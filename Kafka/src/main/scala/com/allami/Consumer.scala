package com.allami


import java.util.concurrent._
import java.util.{Collections, Properties}

import kafka.consumer.KafkaStream
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.allami.Config._
import com.allami.Producer.{producer, topic}

import scala.collection.JavaConversions._


object Consumer  extends App{

  val consumer = Config.ConsumerConf.consumer

  consumer.subscribe(Collections.singletonList("data"))


  val records = consumer.poll(1000)

  for (record <- records) {

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

    val producer =  Config.ProducerConf.producer
    val data = new ProducerRecord[String, String](topic, "localhost", line)
    producer.send(data)

  }



}
