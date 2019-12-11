package org.training.spark.apiexamples

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hduser on 4/16/17.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster("local")

    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("/home/hduser/input_files/sample.txt")

    val wordCountRDD = inputRDD.flatMap(rec => rec.split(" "))
    /*val wordPairRDD =wordCountRDD.map(word => (word,1))
    val wordCountRDD=wordPairRDD.reduceByKey(_ +word => Key _)*/
      .map(word => (word, 1)).reduceByKey(_ + _)
//    wordCountRDD.collect().foreach(println)
//    println(wordCountRDD.collect.toList)
    wordCountRDD.foreach(println)


  }
}
