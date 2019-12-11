package org.training.spark.training

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 6/6/18.
  */
object CsvFileReader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("csv file read")
    val sc = new SparkContext(conf)

    val sqlC = new SQLContext(sc)

    val csvDF = sqlC.read.format("csv").
      option("header","true").
      option("inferSchema","true").
      load("src/main/resources/sales.csv")

    csvDF.printSchema()

    csvDF.show
  }

}
