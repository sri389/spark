package data_Prac_scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 11/2/18.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val sQLContext= new SQLContext(sc)
    val options1 = Map("header"->"true","InferSchema"->"true")

//    val df = sQLContext.read.option("header","true").
println("parquet started")
    val parquetDf = sQLContext.read.options(options1)
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
    parquetDf.printSchema()
    parquetDf.show(10)
    println("parquet is done")

    println()

    println("json started")
    val jsonDf = sQLContext.read.options(options1)
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/flight-data/json/2010-summary.json")
    jsonDf.printSchema()
    jsonDf.show(10)
    println("json ended")


  }

}
