package learning

import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hduser on 8/26/18.
  */
object SparkJsonReader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("json reader")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val schema = StructType(Array(
      StructField("tid",LongType,true) ,
        StructField("cid",LongType,true),
        StructField("itld",IntegerType,true),
        StructField("price",DoubleType,true)
    ))
    //val jsnDF = sqlContext.read.json("src/main/resources/yelp_academic_dataset_business.json")
val jsonDF = sqlContext.read.format("csv")
  .option("mode","DROPMALFORMED")
  .schema(schema)
  .load("src/main/resources/sales.csv")
    jsonDF.printSchema()
    jsonDF.show()
  }
}
