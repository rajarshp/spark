package org.sample.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Create a class TransactionLogs to represent the log data structure schema
case class TransactionLogsforFile (CustomerID:String, CreditCardNo:Long, TransactionLocation:String, TransactionAmount:Int,
                            TransactionCurrency:String, MerchantName:String, NumberofPasswordTries:Int,
                            TotalCreditLimit:Long, CreditCardCurrency:String)


object StreamingCSVFile {

  def main(args: Array[String]): Unit = {
    // create sparkconfig and streaming context

    val sc = new SparkConf().setMaster("local").setAppName("CSVFileProcessing")
    val streamingContext = new StreamingContext(sc,Seconds(10))

    // create sparksession
    val sparkSession = SparkSession.builder.config(sc).getOrCreate()
    import sparkSession.implicits._

    // read the file using stream

    val fileLogStream = streamingContext.textFileStream("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\data")

    //Collecting the records for window period of 60seconds and sliding interval time 20 seconds
    val windowedLogsStream = fileLogStream.window(Seconds(60),Seconds(20));

    // Iterate each RDD and create DF to run the Spark SQL operation

    windowedLogsStream.foreachRDD(
      rdd =>
      {
        val dfLog = rdd.map(x => x.split(",")).map { c => TransactionLogsforFile(c(0), c(1).trim.toLong, c(2), c(3).trim.toInt,
          c(4), c(5), c(6).trim.toInt, c(7).trim.toInt, c(8)) }.toDF()

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
