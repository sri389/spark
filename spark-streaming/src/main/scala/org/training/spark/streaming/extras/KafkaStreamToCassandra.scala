package org.training.spark.streaming.extras

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 1/21/18.
  */
object KafkaStreamToCassandra extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark kafka stream to Cassandra app").
    set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(10))

  val kStream = KafkaUtils.createStream(ssc,
    "localhost:2181",
    "spark_kafka",
    Map("kafka_cassandra" -> 1),
    StorageLevel.MEMORY_ONLY)

  val words = kStream.flatMap(t => t._2.split(" "))

  words.map(line => {val arr = line.split(","); (arr(0).toInt, arr(1)) })
    .saveToCassandra("training","emp", SomeColumns("emp_id","emp_name"))

  words.print()
  ssc.start()
  ssc.awaitTermination()



}
