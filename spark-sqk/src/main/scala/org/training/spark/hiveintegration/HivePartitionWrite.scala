package org.training.spark.hiveintegration

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 11/29/16.
  */
object HivePartitionWrite extends App {

  val conf = new SparkConf().setMaster(args(0)).setAppName("Hive partition write")

  val sc = new SparkContext(conf)

  System.setProperty("javax.jdo.option.ConnectionURL",
    "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
  System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
  System.setProperty("javax.jdo.option.ConnectionUserName", "root")
  System.setProperty("javax.jdo.option.ConnectionPassword", "training")
  System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

  val hc = new HiveContext(sc)

  val loadOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> "|")

  val salesDF = hc.read.format("com.databricks.spark.csv").options(loadOptions).load(args(1))

  salesDF.printSchema()
  salesDF.show

  hc.sql("create table if not exists learning.itemid_partitions(transactionId int, customerId int, amountPaid double) partitioned by(itemId int)")

  hc.setConf("hive.exec.dynamic.partition", "true")
  hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  //salesDF.registerTempTable("temp")
  val salesDF1 = salesDF.withColumnRenamed("itemId", "itemid")
    //salesDF1.printSchema()
  salesDF1.write.mode("append").partitionBy("itemid").saveAsTable("learning.itemid_partitions")
  //salesDF.withColumn("itemid", col("itemId")).registerTempTable("temp")
  salesDF1.registerTempTable("temp")
  hc.sql("insert into table learning.itemid_partitions partition(itemid) select transactionId, customerId, amountPaid, itemid from temp")
}
