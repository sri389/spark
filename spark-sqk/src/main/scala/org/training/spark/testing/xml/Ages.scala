package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.reflect.macros.whitebox

/**
 * Created by hduser on 13/8/16.
 */
object Ages {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new SQLContext(sc)
    val xmlOptions = Map("rowTag" -> "people")
    val df = sqlContext.read.format("com.databricks.spark.xml").options(xmlOptions).load(args(1))
    df.printSchema()
    df.show

    df.show(10,truncate = false)

    val df1 = df.select(col("person"),explode(col("person")).as("person1"))
    df1.printSchema()
    df1.show()

    val df2 = df1.select("person1.name","person1.age.@born","person1.age.#VALUE")

    val df3 = df2.withColumnRenamed("@born","birthdate")
      .withColumnRenamed("#VALUE","age")
        .selectExpr("name as NAME","birthdate as DOB","age as AGE")
      df3.show()

    df3.selectExpr("NAME","DOB","AGE","'ETLAdmin' as created_by","current_timestamp() as created_date")
    df3.withColumn("createDt",current_date()).withColumn("createdBy",lit("ETLADMIN")).show()

    df.withColumn("newPerson",explode(col("person"))).show()

    import sqlContext.implicits._
//    val df1 = df.select(col("age.#VALUE").alias("actual_age"), df("age.@born").as("birthdate"))
//    val groupedCount = df1.groupBy("actual_age").count()
//    groupedCount.show
    /*val df1 = df.select($"age.#VALUE" alias "actual_age", col("age.@born").alias("birthdate"),col("name"))

    df1.selectExpr("actual_age as age","birthdate as borndate").show

    val aggDF = df1.groupBy("actual_age").count

    aggDF.show()

    val aggDF1 = df1.groupBy("actual_age").
      agg(count("actual_age").alias("count"),collect_list("name").alias("names"))

    aggDF1.show*/






  /*  df.select("age.#VALUE", "age.@born").show()
    df.select(df("age.#VALUE").alias("actualage"), df("age.@born").alias("DOB")).show

    //rename the column
    import sqlContext.implicits._
    df.select($"age.#VALUE".alias("acturalAge") , $"age.@born".alias("birthdate")).show()
    val df1 = df.select($"age.#VALUE" alias "actualAge" , $"age.@born" alias "birthdate", $"name")

    df1.show

    df1.groupBy("actualAge").count().show
    val df2 = df1.groupBy(col("actualAge")).agg(count(col("actualAge")).alias("totalCount"))
    df2.show
    df2.where("totalCount > 1").show
    //df.selectExpr("age as actualAge").show()

    df1.registerTempTable("ages")

    sqlContext.sql("select actualAge, count(actualAge), collect_list(name) as person_names from ages group by actualAge").show
*/
    /*println(df2.queryExecution.logical.numberedTreeString)
    println(df2.queryExecution.analyzed.numberedTreeString)
    println(df2.queryExecution.optimizedPlan.numberedTreeString)
    println(df2.queryExecution.executedPlan.numberedTreeString)*/
  }
}
