package org.training.spark.training

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 6/6/18.
  */
object XmlFileReader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("csv file read")
    val sc = new SparkContext(conf)

    val sqlC = new SQLContext(sc)

    val xmlDF = sqlC.read.format("xml").
      option("rowTag","person").
      load("/home/hduser/Projects/spark-sql/src/main/resources/ages.xml")

    xmlDF.printSchema()

    xmlDF.show

    import sqlC.implicits._
    val flatDF = xmlDF.select(xmlDF("age.@born").as("borndate"),
      col("age.#VALUE").alias("age"),$"name".alias("name"))

    flatDF.show()

    val df1 = flatDF.withColumn("dummy", lit("test"))
    df1.show

    val df2 = df1.withColumn("agegroup",
      when(col("age") <= 25,lit("youth")).
      otherwise(when(col("age") > 25 && col("age") < 60,
        lit("adult")).
        otherwise(lit("senior citizen")))
    ).withColumnRenamed("dummy","newColumn")

    val df3 = df2.drop("newColumn")

    df3.show
  // select count(age),max(age) from df3 group by age
    val df4 = df3.groupBy("age").agg(
      count("age").as("count"), max("age").alias("maxage"))
    df4.show

    //select count(age) from df3
   val df5 =  df3.agg(("age","count"))
    df5.show

    // select count(age) from df3 group by age
    val df6 = df3.groupBy("age").count()
    df6.show()
  }

}
