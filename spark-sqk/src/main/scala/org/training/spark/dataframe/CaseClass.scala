package org.training.spark.dataframe


//import org.training.spark.utils.Sales
import org.apache.spark._
//import org.training.spark.utils.Sales

/**
  * Creating Dataframe from case classes
  */
case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

object CaseClass {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("caseclass")
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val rawSales = sc.textFile(args(1))
    val salesRDD = rawSales.filter(line => !line.startsWith("transactionId"))
      .map(_.split(","))
      .map(p => Sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = salesRDD.toDF

    //val salesRdd1 = rawSales.filter(line => !line.startsWith("transactionId"))
    // .map(_.split(","))
    // .map(p => Sales(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    salesDF.printSchema()
    salesDF.show()

    val salesDF1 = sqlContext.createDataFrame(salesRDD)
    salesDF1.printSchema()
    salesDF1.show()

    val salesRdd = salesDF.rdd

    println(salesRdd.collect.toList)

  }
}