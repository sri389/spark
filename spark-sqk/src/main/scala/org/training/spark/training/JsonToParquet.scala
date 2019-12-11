package org.training.spark.training

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 6/23/18.
  */
object JsonToParquet {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(getClass.getName)

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jsonDF = sqlContext.read.json(args(0))

    val filterDF = jsonDF.where("customerId = 1")

    filterDF.write.parquet(args(1))
  }
}
