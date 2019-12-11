package org.training.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sumantht on 6/7/2017.
  */
object NestedJSON extends App {

  val conf = new SparkConf().setAppName("Nested json").setMaster("local")

  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val data = sqlContext.read.json("src/main/resources/testfile_json.json")

  data.printSchema()

  data.show(truncate=false)

  val data1 = data.select(data("__v").alias("col1"), data("_id.$oid").alias("col2"),
  data("created.$date").alias("col3"), data("customerId"), data("deviceName"),
  data("kbReceived.$date").alias("kbReceivedAt"), data("message.echo").alias("message_echo"), data("message"))

  data1.show

  //data1.withColumn("message_battery", explode(data1("message"))).show
}
