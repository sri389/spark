package Interview_Faced

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 11/21/18.
  */
object AddincColumnStartWith {

  def main (args :Array[String]):Unit={

    val conf = new SparkConf().setAppName("StartsWith").setMaster("local")

    val sc = new SparkContext(conf)

    val sparksql= new SQLContext(sc)

    val df = sparksql.read.format("csv").option("header","true").option("inferschema","true").
      load("/home/hduser/Projects/spark-sql/src/main/resources/Interview_Sample_Data/Vehicles_Data")

    df.show()

    val df1 = df.withColumn("requirement",
      when(col("VehicleId").startsWith("T"), "Truck")
      .otherwise(when(col("VehicleId").like("D%"), "Door")
      .otherwise("AnyThing"))).show()
//    val df2 = df.withColumn("rownumber",row_number() over(Window.partitionBy("VehicleId"))).show()
  }

}
