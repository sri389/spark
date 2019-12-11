package org.training.spark.streaming.practiv

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hduser on 12/5/18.
  */
object TextStream extends App{

  val conf = new SparkConf().setAppName("socket stream word count")
    .setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(20))

  val linesDStream  = ssc.textFileStream("/home/hduser/input_files/fileStream")

  val wordsDStream = linesDStream.flatMap(line => line.split(" ")).map(w => (w, 1))

  val wordCountDStream = wordsDStream.reduceByKey(_ + _)

  wordCountDStream.print()

  ssc.start()
  ssc.awaitTermination()



}
