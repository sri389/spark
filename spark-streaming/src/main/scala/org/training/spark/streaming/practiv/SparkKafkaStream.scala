package org.training.spark.streaming.practiv

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
/**
  * Created by hduser on 12/12/18.
  */
object SparkKafkaStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("spark_kafka_stream")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host","localhost")

    val ssc = new StreamingContext(conf, Seconds(10))

    val KStream = KafkaUtils.createStream(ssc,
      "localhost:2181",
      "spark_kafka_consumer",
      Map("spark_kafka" -> 1))

    ssc.checkpoint("/home/hduser/checkpoint")

    val wordsStream = KStream.map(t=>t._2).flatMap(_.split(" "))

    val wordCount = wordsStream.map(w=>(w,1))
      .updateStateByKey((values:Seq[Int],rc:Option[Int])=>{
        val newCount = values.sum+rc.getOrElse(0)
        Some(newCount)
      })

    wordCount.saveToCassandra("spark","wordcount",SomeColumns("word","cnt"))
                              // 1st key sapce, 2nd argument

    ssc.start()
    ssc.awaitTermination()


  }

}
