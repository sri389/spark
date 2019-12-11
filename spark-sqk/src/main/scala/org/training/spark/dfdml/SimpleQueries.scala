package org.training.spark.dfdml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * Created by hduser on 26/8/15.
 */
object SimpleQueries {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("simplequeries")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val opt = Map("header" -> "true", "inferSchema" -> "true")
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").options(opt).load(args(1))
    salesDf.cache()

    salesDf.describe().show
    println()

    val df2 = salesDf.withColumn("newColumn",col("transactionId")%10)
    df2.show()
    println()

    val df1 = df2.drop("newColumn")
    df1.show()
    println()



    //Projection
    val itemIds = salesDf.select("itemId", "customerId").withColumn("flag", col("itemId")+2)
      .withColumnRenamed("flag","newitemId")
    println("Projecting itemId column from sales")
    itemIds.show()


    //Aggregation
    val totalSaleCount = salesDf.agg(("transactionId","count"))
    //val totalSaleCount = sqlContext.sql("SELECT count(transactionId) FROM sales")
    println("Counting total number of sales")
    totalSaleCount.show()

    val testDf = salesDf.withColumn("customer_type",
      when(col("amountPaid") > 2000, lit("prime")).otherwise(lit("non prime")))

    //Group by
    val totalAmtPerCustomer = salesDf.groupBy("customerId").agg(("amountPaid", "sum"))
    salesDf.groupBy("customerId").agg(sum(col("amountPaid")).alias("total amount"),collect_list("itemId"),collect_list("transactionId")).show
    println("Customer wise sales amount")
    totalAmtPerCustomer.show()

    Thread.sleep(100000)
  }

}
