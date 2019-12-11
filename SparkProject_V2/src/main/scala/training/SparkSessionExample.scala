package training

import org.apache.spark.sql.SparkSession

/**
  * Created by hduser on 7/1/18.
  */
object SparkSessionExample {

  case class Customer(customerId:String, customerName:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local").
      appName("First spark v2 app").getOrCreate()

    val df = spark.read.option("header","true").
      csv("src/main/resources/customers.csv")

    df.select("customerId").show

    import spark.implicits._
    val ds = df.as[Customer]
    ds.map(row => row.customerId).show()
    ds.select("customerId").show

  }
}
