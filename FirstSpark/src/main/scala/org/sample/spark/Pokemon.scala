package org.sample.spark

import org.apache.spark.{SparkConf, SparkContext}

object Pokemon {


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("pokemon"))
    sc.setLogLevel("ERROR")

    val data = sc.textFile("pokemon.csv")

    // check if data is loaded or not
    println("Pokemon Details")
    data.take(2).foreach(println)

    // remove header
    val header = data.first()
    val pokemon = data.filter(x => !x.equals(header))

    // filter water pokemon
    val water = pokemon.filter(_.contains("Water"))

    // print water pokemon
    println("Water Pokemon")
    water.take(2).foreach(println)

    // find max defence score
    val maxDefence = pokemon.map(_.split(",")).map(x => x(7).toInt).max()
    println("Max Defence Score is: " + maxDefence)

    // find top 5 defence point
    val topDefence = pokemon.map(_.split(",")).map(x => x(7).toInt).sortBy(x => x)
    println("Top 5 Defence Score is: ")
    topDefence.top(5).foreach(println)

    // pokemon wtih defence point and name

    val nameWithDef = pokemon.map(_.split(",")).map(x => x(1) + "," + x(7).toInt)

    // show data

    nameWithDef.take(2).foreach(println)

    // find pokemon with highest defence score
    val maxDefPok = nameWithDef.filter(_.contains("230")).map(_.split(",")).map(x => (230, x(0))).reduceByKey((x, y) => x + y)
    println("Total pokemon with highest defence score " + nameWithDef.filter(_.contains("230")).count())
    println("Pokemon with highest defence score ")
    maxDefPok.collect().foreach(println)

    // filter tuple
    println("Filter Tuple")
    val tup = pokemon.map(_.split(",")).map(x => (x(1), x(7).toInt))
    val tupFilterMax = tup.filter(_._2 == 230) // _._2 = 2nd value of the tuple
    tupFilterMax.collect().foreach(println)

    // filter data based on the 2nd value of the array off tuple (RDD)

    val tupFilter = tup.filter(x => x._2 > 45 && x._2 < 50) // x._2 = 2nd value of the tuple
    tupFilter.collect().foreach(println)

    // find pokemon with least defence strength
    val minDef = pokemon.map(_.split(",")).map(x => x(7).toInt).distinct().takeOrdered(5)
    println("Least defence score")
    minDef.foreach(println)

    // sortBy operation
    println("sortBy Default mode")
    pokemon.map(_.split(",")).map(x => x(7).toInt).sortBy(x => x).take(10).foreach(println)
    println("sortBy Ascending = true")
    pokemon.map(_.split(",")).map(x => x(7).toInt).sortBy(x => x, true).take(10).foreach(println)
    println("sortBy Ascending = false")
    pokemon.map(_.split(",")).map(x => x(7).toInt).sortBy(x => x, false).take(10).foreach(println)

  }
}
