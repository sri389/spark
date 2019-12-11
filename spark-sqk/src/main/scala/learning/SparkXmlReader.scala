package learning

import breeze.linalg.where
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.training.spark.testing.HiveMetaDataClient

/**
  * Created by hduser on 8/25/18.
  */
object SparkXmlReader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("xml reader")
    val sc = new SparkContext(conf)

    val sqlC = new HiveContext(sc)

    val xmlDF = sqlC.read.format("xml").
      option("rowTag", "book").
      load("src/main/resources/books-nested-array.xml")
    xmlDF.show()
    xmlDF.printSchema()

    val flatDF = xmlDF.withColumn("new_publish_date",
      explode(col("publish_date"))).drop("publish_date")

    flatDF.show(100)

    /*val versionedDf = flatDF.withColumn("version",
      row_number().over(Window.partitionBy("@id"),
        orderBy(desc("new_publish_date"))))
    where
*/
    xmlDF.registerTempTable("test")

    val flatSqlDf = sqlC.sql("select *,"+
    "explode(publish_date) as publish_date from test")

    flatSqlDf.show(50)

/*
    import sqlC.implicits._

    val newDF = xmlDF.select(xmlDF("age.#VALUE").as("actualage"),
      col("age.@born").alias("borndate"),
    $"name")

    newDF.show()
    newDF.printSchema()

    val aggDF = newDF.groupBy("actualage").count()
    aggDF.show

    val categoryDF = newDF.withColumn("age_category",
      when(col("actualage") <= 20,"youth").otherwise("adult"))
      .withColumnRenamed("actualage","age")

    categoryDF.show

    val renamedDF = categoryDF.selectExpr("age as age",
    "borndate as birthdate","name as fullname","age_category")

    renamedDF.show()

    val renamedDF1 = categoryDF.selectExpr("age as age",
      "borndate as birthdate","upper(name) as name","age_category")

    renamedDF1.show()

    val multiageDF = renamedDF1.groupBy("age").agg(count("age"),
      collect_list("name"))

    multiageDF.show()

*/



  }
}
