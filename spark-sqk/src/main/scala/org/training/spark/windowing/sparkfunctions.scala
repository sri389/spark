package org.training.spark.windowing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 9/22/18.
  */
object SparkFunctions extends App{

  val conf = new SparkConf().setMaster("local").setAppName("SparkFunctions")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  val salesDf = sqlContext.read.format("com.databricks.spark.csv")
    .option("header","true")
    .option("inferschema","true")
    .load(args(0))
  salesDf.registerTempTable("sales")
  salesDf.withColumn("discount",
    when(col("amountPaid") .between(2000, 5000), col("amountPaid")*.9)
      .when(col("amountPaid")>5000, col("amountPaid")*.8)
      .otherwise(col("amountPaid"))).show()

  val newSalesDf = salesDf.groupBy("customerId")
    .agg(count("amountPaid").as("count"), sum("amountPaid")
      .as("sum"),min("amountPaid").as("minSpent") ,max("amountPaid")
      .alias("maxspent"), avg("amountPaid").alias("avgSpent"),
      collect_list("amountPaid").alias("transaction"),
      collect_set("amountPaid").alias("uniqueItems"))
  newSalesDf.show()

  newSalesDf.select(col("customerId"),explode(col("transaction"))
    .alias("transaction")).show()

  //val concatDf = salesDf.select(concat(col("transactionId"), lit("|"), col("customerId")))

  salesDf.withColumn("allcol",concat_ws("|",col("transactionId"), col("customerId"), col("itemId"), col("amountPaid"))).show()
  //concatDf.show()

  salesDf.withColumn("rowNumber", row_number().over(Window.orderBy("transactionId"))).show()

  // val spec = Window.orderBy(col("amountPaid").desc)
  //salesDf.
  salesDf.withColumn("rank", rank().over(Window.orderBy("amountPaid"))).show()

  salesDf.withColumn("DenseRank", dense_rank().over(Window.orderBy("amountPaid"))).show()

  salesDf.withColumn("rank", rank().over(Window.partitionBy("customerId").orderBy(desc("amountPaid")))).show()
  // .filter("rank = 1").show()
  salesDf.withColumn("rank", dense_rank().over(Window.partitionBy("customerId").orderBy(desc("amountPaid")))).show()
  //.filter("rank = 2").show()

  def concat(x:String, y:String) = x+"-"+y
  val concatUDF = udf(concat _)

  salesDf.select(concatUDF(col("transactionId"), col("itemId"))).show()
}
