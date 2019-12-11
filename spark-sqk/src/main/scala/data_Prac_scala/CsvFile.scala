package data_Prac_scala

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 10/31/18.
  */
object CsvFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Csv_FIle_Read").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    val hivecontext = new HiveContext(sc)
    val options1 = Map("header"->"true","InferSchema"->"true")
    // modes -> permissive,dropMalformed,failFast

    val myManualSchema =
       new StructType(Array(
          new StructField("DEST_COUNTRY_NAME", StringType, true),
          new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
          new StructField("count", LongType, false) ))

    //csv Read

    val csvDf = sqlcontext.read.format("csv").options(options1).option("dateFormat","yyyy-MM-dd")
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/bike-data/201508_station_data.csv")

    csvDf.printSchema()
    csvDf.show(5,true)

    // json Read

    val jsonDf = sqlcontext.read.format("json").options(options1)
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/flight-data/json/2010-summary.json")
    jsonDf.printSchema()
    jsonDf.show(10)

    // ORC Read

    val orcDf = hivecontext.read.format("orc").options(options1)
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/flight-data/orc/2010-summary.orc/part-r-00000-2c4f7d96-e703-4de3-af1b-1441d172c80f.snappy.orc")
    orcDf.printSchema()
    orcDf.show(10)

    // parquet Read

    val parquetDf = sqlcontext.read.format("parquet").options(options1)
      .load("/home/hduser/Projects/spark-sql/src/main/resources/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
    parquetDf.printSchema()
    parquetDf.show(10)

    // CSV write
    val option1= Map("header"-> "true","schema" -> "myManualSchema")//,"codec" -> "bzip2")
    //mode -> append, overwrite,errorIfExist,ignore
    //Accepted modes are 'overwrite', 'append', 'ignore', 'error'.
    val csvwriteDF=csvDf.write.mode("overwrite").format("com.databricks.spark.csv").options(option1).option("delimiter","\t")
      .option("dateFormat","yyyy-MM-dd").save("src/main/resources/my-tsv-file.tsv")

    //append mode is not supported for csv
    //compression (or) codec -> none(default),bzip2,snappy,uncompressed,deflate,lz4.

    //Json write
    val jsonwriteDF=jsonDf.write.mode("overwrite").format("json").option("compression","snappy")
      .save("src/main/resources/json-output")

    //Parquet Write
    val parquetWriteDF= parquetDf.write.mode("ignore").format("parquet").options(options1)
      .save("src/main/resource/parquet-output")

    //Orc write
    val orcWriteDF= orcDf.write.mode("append").format("orc").options(options1)
      .save("src/main/resource/orc-output")

    //MysqlRead
    val dbDataFrame = sqlcontext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ecommerce").option("dbtable", "sales").option("user", "root")
      .option("password","training")
      .option("numPartitions", "1").load()
    dbDataFrame.printSchema()
    dbDataFrame.show(10)

    //MysqlWrite
    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "training")

    dbDataFrame.withColumn("test1", lit(5)).write.
      mode("overwrite").
      jdbc("jdbc:mysql://localhost:3306/ecommerce", "sales_training1", properties)

    //Partitioning
    val partitionDF=jsonDf.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
      .save("src/main/resource/parquet_part")

    //Bucketing
    /*val bucketDF=orcDf.write.format("parquet").mode("overwrite")
      .bucketBy(5,"DEST_COUNTRY_NAME" ).saveAsTable("bucketedFiles")*/


  }

}
