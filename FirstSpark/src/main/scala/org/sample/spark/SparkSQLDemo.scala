package org.sample.spark

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class Students(id:Int,gender:String,group:String,degree:String,courseType:String,status:String,math:Int,english:Int,bio:Int)

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {

    // for dataframe create SparkSession
    val sc =  new SparkContext(new SparkConf().setMaster("local").setAppName("SparkJoin"))
    sc.setLogLevel("ERROR")

  // sparksession is needed to work on sqlrk dataframe/sql
    val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    // this import we need to create RDD to DataFrame
    import sparkSession.implicits._

    // read data
    val data = sc.textFile("students.csv")

    // extract header

    val header = data.first()

    //  create RDD
    val studentRDD = data.filter(line => !line.equals(header)).map(_.split(",")).map(f => Students(f(0).toInt,
      f(1),f(2),f(3),f(4),f(5),f(6).toInt,f(7).toInt,f(8).toInt))

    // create the dataframe from the RDD

    val df = studentRDD.toDF()

    // display data
    df.show(5)

    // display only degree
    df.select("degree").show(3)

    // registered RDD for SparkSQL operation
    df.createOrReplaceTempView("students")

    // run sql queries
    val sqlSelect = sparkSession.sql("select * from students limit 5")
    sqlSelect.show()

    // create dataframe from RDD and define column names

    val df1 = data.filter(l=> !l.equals(header)).map(_.split(",")).collect{case Array(id,gender,group,
      degree,ctype,status,math,eng,bio) =>
    (id,gender,group,degree,ctype,status,math,eng,bio)}
    .toDF("id","gender","group","degree","type","status","math","eng","bio")

    // display data

    df1.show(3)

    df1.groupBy("group").count().show()

    // create dataset from RDD
    println("create dataset from RDD")
    val ds = studentRDD.toDS()
    ds.show(5)

    // create dataset from dataframe
    println("create dataset from DataFrame")
    Encoders.product[Students]
    val ds1 = df.as[Students]
    ds1.show(2)

    // create paraquet file with overwrite

    ds1.coalesce(1).write.mode(saveMode = SaveMode.Overwrite).parquet("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\paraquet")

    // read paraquet file
    println("read paraquet file")
    val para = sparkSession.read.parquet("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\paraquet")
    para.take(2).foreach(println)

    // create JSON file with overwrite

    ds1.coalesce(1).write.mode(saveMode = SaveMode.Overwrite).json("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\json")

    // read JSON file
    println("read JSON file")
    val js = sparkSession.read.json("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\json")
    js.take(2).foreach(println)

    // create AVRO file with overwrite

    ds1.coalesce(1).write.mode(saveMode = SaveMode.Overwrite).format("avro").save("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\avro")

    // read AVRO file
    println("read AVRO file")
    val avrofile = sparkSession.read.format("avro").load("C:\\Users\\rajar\\IdeaProjects\\FirstSpark\\avro")
    avrofile.take(2).foreach(println)
  }
}
