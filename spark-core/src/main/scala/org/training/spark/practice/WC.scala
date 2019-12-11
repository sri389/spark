package org.training.spark.practice

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 9/27/18.
  */
object WC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local")

    val sc = new SparkContext(conf)

    val l = List(1,2,3,4)

    val rdd = sc.parallelize(l)

    rdd.collect().foreach(println)

    val fileRdd = sc.textFile("/home/hduser/input_files/EmployeeData.csv")

    val words = fileRdd.flatMap(rec => rec.split(","))
    val wordPairRDD =words.map(word => (word,1))
    val wordCountRDD=wordPairRDD.reduceByKey(_ + _)

    wordCountRDD.collect().foreach(println)


  }

}
