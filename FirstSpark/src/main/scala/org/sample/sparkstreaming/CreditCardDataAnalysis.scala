package org.sample.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class CustomerRef(CustomerName:String, DOB:String,SSN:String, City:String, State:String, ZipCode:Long,
                       ExistingProducts:String, OtherBankProducts1:String, CreditSpent1:Long,
                       OtherBankProducts2:String, CreditSpent2:Long, OtherBankProducts3:String,
                       CreditSpent3:Long, DefaulterFlag:String)

case class CardApplication(CustomerName:String, DOB:String, SSN:String, MailID:String,
                           PhoneNumber:Long, City:String, State:String, ZipCode:String, CreditLimit:Long)


object CreditCardDataAnalysis {
  def main(args: Array[String]): Unit = {
    val sc =new SparkContext(new SparkConf().setMaster("local").setAppName("CreditCard"))
    sc.setLogLevel("ERROR")
    val streamingContext = new StreamingContext(sc,Seconds(30))

    val sparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    import sparkSession.implicits._

    // read customer reference data

    val customerRefData = sc.textFile("CustomerRefererenceData.csv").map(_.split(","))
    .map(c => CustomerRef(c(0),c(1),c(2),c(3),c(4),c(5).toLong,c(6),c(7),c(8).toLong,c(9),c(10).toLong,c(11),
      c(12).toLong,c(13))).toDF()

    //println("TEST FILE DATA")
    //customerRefData.take(2).foreach(println)

    customerRefData.createOrReplaceTempView("customerrefdata")


    val kafkaParam = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test_kafka",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val cardApplicationDataRDD = KafkaUtils.createDirectStream[String,String](streamingContext,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array("CreditCardApplicationData"),kafkaParam))

    cardApplicationDataRDD.foreachRDD(
      rdd =>{
        rdd.take(2).foreach(println)
        val cardApplicationDetails = rdd.map(x => x.value().split(",")).map{r => CardApplication(r(0),r(1),r(2),
          r(3),r(4).toLong,r(5),r(6),r(7),r(8).toLong)}.toDF()
        println("TEST KAFKA DATA")
        cardApplicationDetails.take(2).foreach(println)
        cardApplicationDetails.createOrReplaceTempView("cardapplicationdetails")

        val report = sparkSession.sql("select T3.SSN,T3.CustomerName,T3.Status from (select T2.SSN,T2.CustomerName," +
          " T2.CreditSpent1, T2.CreditSpent2, T2.CreditSpent3, T1.CreditLimit,T2.DefaulterFlag, " +
          " case " +
          "    when T2.DefaulterFlag = 'Y' then 'Rejected'" +
          "    when (T2.CreditSpent1+T2.CreditSpent2+T2.CreditSpent3) > T1.CreditLimit then 'Rejected'" +
          "    else 'Approved'" +
          " end as Status from cardapplicationdetails T1 join customerrefdata T2 on T1.SSN = T2.SSN) T3")

        report.show()
      }
    )

    // start streaming
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
