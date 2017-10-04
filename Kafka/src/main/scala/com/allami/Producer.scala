package com.allami

import java.util.{Date, Properties}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.{Try, Success, Failure}



object Producer extends App {

  val producer =  Config.ProducerConf.producer

  val topic=Config.ProducerConf.topic
  val filename = "/opt/host.log"
  for (line <- Source.fromFile(filename).getLines) {

    val datetime=line.split(" ")(0)+" "+line.split(" ")(1)
    val timestamp=getTimestamp(datetime).get
    val  data = new ProducerRecord[String,String](topic,3,timestamp,"ligne", line)
    producer.send(data)

  }

  producer.close()

  def getTimestamp(s: String) : Option[Long] = s match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat("yyyy/MM/dd' 'HH:mm:ss")
      Try(format.parse(s).getTime) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }

}