package org.training.spark.training

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 12/4/18.
  */
object SqlCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("spark_jdbc_write").
      setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "training")
    /*salesDf.withColumn("test1", lit(5)).write.
      mode("overwrite").
      jdbc("jdbc:mysql://localhost:3306/ecommerce", "sales_training", properties)
*/


  }

}
