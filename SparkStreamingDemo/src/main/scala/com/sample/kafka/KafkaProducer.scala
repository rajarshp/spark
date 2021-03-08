package com.sample.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.Source

object KafkaProducer {

  var producer : KafkaProducer[String, String] = null

  def createKafkaProducer(): KafkaProducer[String, String] ={
    // create kafka properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // create producer
    producer = new KafkaProducer[String,String](props)

    return producer
  }


  def writeToKafkaTopic(topicName:String,record:String): Unit ={

    // create producer receord
    val precord = new ProducerRecord[String,String](topicName,record.split(",")(0),record)

    // send to kafka

    producer.send(precord)
  }

  def main(args: Array[String]): Unit = {
    // create producer
    println("Starting Producer")
    producer = createKafkaProducer()

    // read file
    println("reading data from file")
    val fileData = Source.fromFile("CreditCardApplicationData.csv")

    for(line <- fileData.getLines())(
      writeToKafkaTopic("CreditCardApplicationData",line)
    )
    println("Data sent to kafka")
    producer.close()
  }

}
