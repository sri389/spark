package org.training.spark.streaming.learning

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hduser on 4/5/18.
  */
object SocketStream extends  App {

  val ssc = new StreamingContext(args(0), "wordcount", Seconds(10))
  val lines = ssc.socketTextStream(args(1),args(2).toInt)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  ssc.start()             // Start the computation
  ssc.awaitTermination()

}
