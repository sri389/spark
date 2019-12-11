package org.spark.training.apiexamples

import org.apache.spark.sql.SparkSession

/**
  * Logical Plans for Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._


    //read data from text file

    val df = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv")
    val ds = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/sales.csv").as[Sales]


    val selectedDF = df.select("itemId")

    val selectedDS = ds.map(_.itemId)

    selectedDF.show

    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
    println()
    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)


  }

}
