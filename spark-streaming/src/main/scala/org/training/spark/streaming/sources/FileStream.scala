package org.training.spark.streaming.sources

import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Created by madhu on 3/7/14.
 */
object FileStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(args(0), "FileStream", Seconds(20))

    ssc.sparkContext.setLogLevel("ERROR")
    val lines = ssc.textFileStream(args(1))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
