package com.allami.kafka.streaming

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KStreamBuilder
import java.util.Arrays
import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KeyValueMapper
import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KTable
import java.util
import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStreamBuilder, KeyValueMapper}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Reducer
import com.typesafe.scalalogging._
import java.util
import java.util.{Date, Locale}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream

import com.allami.Consumer.logger

object Consumer  extends  App with LazyLogging{



  val builder = new KStreamBuilder()
  val sourceStream = builder.stream("data")
  val config=Config.config

  val Longerde = Serdes.Long()
  val stringSerde = Serdes.String()

  val error : KStream[String, String]  = builder.stream("error")

  val warn : KStream[String, String]  = builder.stream("warn")

  val info : KStream[String, String]  = builder.stream("info")



  logger.info("*****************errors******************** ")

  val showErrors : KTable[String, Long] =error.groupByKey().count()
  showErrors.print(stringSerde, Longerde)


  logger.info("*****************Infos******************** ")

  val showInfo : KTable[String, Long] =info.groupByKey().count()
  showInfo.print(stringSerde, Longerde)


  logger.info("*****************Warnings******************** ")

  val showWarning : KTable[String, Long] =warn.groupByKey().count()
  showWarning.print(stringSerde, Longerde)


  val streams: KafkaStreams = new KafkaStreams(builder, config)
  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(10, TimeUnit.SECONDS)
  }))


  /*

.map[String, String] {
    new KeyValueMapper[String, String, KeyValue[String, String]] {
      override def apply(key: String, value: String): KeyValue[String, String] = {
        println(value)
        new KeyValue(key, "1")
      }
    }
  }


reduce(
       new Reducer[String]{
         override def  apply( aggValue :String,  newvalue:Long): String = {
           return newvalue + newvalue.toString
         }
       },
       "dummy-aggregation-store")

  val counts: KTable[String, Long]=textLines.map[String, String] {
    new KeyValueMapper[String, String, KeyValue[String, String]] {
      override def apply(key: String, value: String): KeyValue[String, String] = {
        println(value)
        new KeyValue(key, value)
      }
    }
  }.groupBy((key, value) => (key)).count()
    counts.to(stringSerde, integerSerde,"show")

//util.Arrays.asList(errors.toLowerCase().split("\\W+")

  //val errors : KTable[String, String]   = info.groupBy((key, word) => key).count("Counts")
  //errors.to("info")

 // val errors : KTable[String, Long] = info.map(a=>(a,1L)).groupBy((key, word) => (key,word))

//(textLine: String) => util.Arrays.asList(textLine.toLowerCase.split("\\W+")))

  val originalStream = builder.stream("info")



  val mappedStream =originalStream.map[String, Long] {

  }.groupBy((key, value) => key)
  mappedStream.count().to("show")


  val mappedStream =info.map[String, String] {
    new KeyValueMapper[String, String, KeyValue[String, String]] {
      override def apply(key: Long, value: String): KeyValue[Long, String] = {
        new KeyValue(key, value)
      }
    }
  }.groupBy((key, value) => key)
  mappedStream.count().to("show")

  val mappedStream =info.map[String, Integer] {
    new KeyValueMapper[String, Integer, KeyValue[String, Integer]] {
       override def apply(key: String, value: Integer): KeyValue[String, Integer] = {
        new KeyValue(key, key.length)
      }
    }
  }.groupBy((key, word) => key)
  mappedStream.count().to("show")

*/

  //.count("show")

   // var a= errors.to("show")

   // errors.count.to("show")



  // val builder = new KStreamBuilder()
  // val sourceStream = builder.stream[String, String]("TextLinesTopic")
  // .groupBy((_,word)=>word).count()
  //.count("Counts")





/*
  val counts = errors.groupBy(new KeyValueMapper[String, String, KeyValue[String, String]]() {
    override def apply(key: String, value: String): KeyValue[String, String] = { // TODO Auto-generated method stub
      KeyValue.pair(key, value)
    }
  }, Serdes.String, Serdes.String).count("count")

     // val values=errors.groupBy((k,v)=>v)
  counts.print()
  counts.to("info")
*/

  //mapValues(k => k.split("")(0))
  //values.through("temp").

 //   toStream.to("autre")


}
