package learning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 5/24/18.
  */
object WordCount extends App {
  val conf = new SparkConf().
    setMaster("local").
    setAppName("wordcount app")

  val sc = new SparkContext(conf)

  val fileRDD = sc.textFile("/home/hduser/input_files/test.txt")

  val wordsRDD = fileRDD.flatMap(rec => rec.split(" "))

  val wordPairRDD = wordsRDD.map(word => (word,1))

  val wordCountRDD = wordPairRDD.reduceByKey(_ + _)

  wordCountRDD.collect().foreach(println)
}








