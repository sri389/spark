package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by hduser on 13/8/16.
 */
object Books {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new HiveContext(sc)
    val xmlOptions = Map("rowTag" -> "book")
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .options(xmlOptions)
      .load(args(1))
    df.printSchema()
    df.show()

    /*val flatdf = df.withColumn("publishdate", explode(df("publish_date")))
    flatdf.show()

    flatdf.registerTempTable("sample")

    //sql way of applying window operations
    val versiondf1 = sqlContext.sql("select *," +
      "row_number() over (partition by title order by publishdate) as version from sample")
    versiondf1.show()

    //dataframe dsl api way
    val versiondf = flatdf.withColumn("version",
      row_number().over(Window.partitionBy("title").orderBy(desc("publishdate"))))
    versiondf.show*/

   /* val df1 = df.withColumn("publish_date", explode(df("publish_date")))
      .withColumnRenamed("@id","id").withColumn("status", lit("processed"))
    df1.show()*/
    //df.show(truncate = false) //passing value to default argument using named argument
  }
}
