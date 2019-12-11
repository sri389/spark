package org.training.spark.streaming.learning

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf

/**
  * Created by hduser on 1/26/18.
  */
object KafkaStreamToCassandra extends App{

  val conf = new SparkConf().setAppName("kafka stream spark to cassandra app").
    setMaster(args(0)).set("spark.cassandra.connection.host","localhost")

  val ssc = new StreamingContext(conf,
    Seconds(10))

  val kStream = KafkaUtils.createStream(ssc,
  "localhost:2181", "kafka_spark_cassandra",
    Map("spark_cassandra" -> 1))

  val record = kStream.map(t => t._2)

  val dbRecord = record.map(row => {
    val arr = row.split(",")
    (arr(0).toInt, arr(1))
  })

  dbRecord.saveToCassandra("kafka_spark", "employee", SomeColumns("empid","empname"))

  ssc.start()
  ssc.awaitTermination()
}
