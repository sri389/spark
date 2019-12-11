package org.training.spark.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * Created by hduser on 7/1/18.
  */
object SocketStructuredStreaming extends App {

  val spark = SparkSession.builder().
    appName(getClass.getName).
    master("local").getOrCreate()

  // Creating DataFrame representing the stream of input lines from connection to localhost:50050
  val lines = spark.readStream.
    format("socket").
    option("host","localhost").
    option("port", "50050").
    load()

  import spark.implicits._

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    
      
      //.trigger(Trigger.ProcessingTime(60000))
    .start()

  query.awaitTermination()

}
