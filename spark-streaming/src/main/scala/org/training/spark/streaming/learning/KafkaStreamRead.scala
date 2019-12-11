package org.training.spark.streaming.learning

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hduser on 1/25/18.
  */
object KafkaStreamRead extends App {

  val ssc = new StreamingContext(args(0),
    "read kafka stream", Seconds(15))

  val kStream = KafkaUtils.createStream(ssc,
  "localhost:2181", "spark_kafka_streaming",
    Map("kafka_training" -> 1))

  kStream.map(t => t._2).print

  ssc.start()
  ssc.awaitTermination()


}
