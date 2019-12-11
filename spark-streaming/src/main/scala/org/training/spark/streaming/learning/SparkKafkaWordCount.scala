package org.training.spark.streaming.learning

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hduser on 6/26/18.
  */
object SparkKafkaWordCount {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(args(0), "spark kafka stream", Seconds(10))

    val recStream = KafkaUtils.createStream(ssc, "localhost:2181",
      "spark_kafka_test_consumer", Map("topic1" -> 1))

    val msgsDStream = recStream.map(t => t._2)

    val wordsStream = msgsDStream.flatMap(_.split(" ")).
      map(word => (word,1))

    val wordCount = wordsStream.reduceByKey(_ + _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
