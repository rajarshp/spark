package org.sample.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Create a class TransactionLogs to represent the log data structure schema
case class TransactionLogs (CustomerID:String, CreditCardNo:Long, TransactionLocation:String, TransactionAmount:Int,
                            TransactionCurrency:String, MerchantName:String, NumberofPasswordTries:Int,
                            TotalCreditLimit:Long, CreditCardCurrency:String)

object ProcessingKafkaLogs {

  def main(args: Array[String]): Unit = {

    // create main entry point for spark streaming - StreamingContext
    val sparkConfig = new SparkConf().setMaster("local").setAppName("KafkaStreaming")
    val streamingContext = new StreamingContext(sparkConfig,Seconds(10))

    val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    import sparkSession.implicits._  // to convert RDD to DF

    // kafka properties
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test_kafka",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //consume the messages from Kafka topic and create DStream

    val logStream = KafkaUtils.createDirectStream[String,String](streamingContext,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array("kafkalog"),kafkaParams))

    //Convert each RDD in to a dataframe using reflection method

    logStream.foreachRDD(
      rdd =>
        {
          val dfLog = rdd.map(x => x.value().split(",")).map { c => TransactionLogs(c(0), c(1).trim.toLong, c(2), c(3).trim.toInt,
            c(4), c(5), c(6).trim.toInt, c(7).trim.toInt, c(8)) }.toDF()

          dfLog.take(2).foreach(println)

          //register temporary table on Dataframe.//register temporary table on Dataframe.

          dfLog.createOrReplaceTempView("CustomerTransactionLogs")
          //sql query validating all business rules to detect fraud.
          val dfFraud = sparkSession.sql("Select CustomerID, CreditCardNo, TransactionAmount, " +
            "TransactionCurrency, NumberofPasswordTries, TotalCreditLimit, CreditCardCurrency from " +
            "CustomerTransactionLogs where NumberofPasswordTries > 3 OR TransactionCurrency != CreditCardCurrency " +
            "OR ( TransactionAmount * 100.0 / TotalCreditLimit ) > 50")

          dfFraud.show()
        }
    )

    // start streaming
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
