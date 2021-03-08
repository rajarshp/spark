package org.sample.spark

import org.apache.spark.{SparkConf, SparkContext}

// create case class

case class Student(id:Int,name:String)
case class Age(id:Int,age:Int)

object SparkJoinExample {
  def main(args: Array[String]): Unit = {
    val sc =  new SparkContext(new SparkConf().setMaster("local").setAppName("SparkJoin"))
    sc.setLogLevel("ERROR")

    val file1 = sc.textFile("1.txt")
    val file2 = sc.textFile("2.txt")



    // map data

    val studentRDD = file1.map(_.split(",")).map(x=>(x(0),Student(x(0).toInt,x(1))))
    val ageRDD = file2.map(_.split(",")).map(x=>(x(0),Age(x(0).toInt,x(1).toInt)))

    // display the data
    println("Student Details")
    studentRDD.take(2).foreach(println)
    println("Age Details")
    ageRDD.take(2).foreach(println)

    //Spark Inner join

    println("Inner Join")

    studentRDD.join(ageRDD).collect().foreach(println)

    println("Right Outer Join")
    studentRDD.rightOuterJoin(ageRDD).collect().foreach(println)

    println("Left Outer Join")
    studentRDD.leftOuterJoin(ageRDD).collect().foreach(println)
  }
}
