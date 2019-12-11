package org.training.spark.training

/**
  * Created by sumantht on 11/8/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)

    val l = List("Learning Scala, Learning Spark, Running Spark with Scala", "Hello Scala", "Hello Spark")
    val rdd = sc.parallelize(l)

    val rd1 = Source.fromFile("/home/hduser/input_files/sample.txt").getLines().toList
    val rd2 = sc.parallelize(rd1)
    rd2.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_).foreach(println)
println()
    rdd.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_).foreach(println)
  }

}
