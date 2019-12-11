package learning

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 8/29/18.
  */
object SparkJdbcReader {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("jdbc reader")

    val sc =new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jdbcProps = Map("url"->"jdbc:mysql://localhost:3306/ecommerce",
      "dbtable"->"(select * from sales where amountPaid > 500)t",
      "user"->"root",
      "password"->"training",
      "partitionColumn" -> "transactionId",
      "lowerbound" -> "111",
      "upperbound" -> "124",
      "numPartitions" -> "4")
    val jdbcDF = sqlContext.read.format("jdbc").options(jdbcProps).load()
    jdbcDF.show()
    jdbcDF.printSchema()
    println(jdbcDF.rdd.getNumPartitions)

    //jdbcDF.write.mode(SaveMode.Append)'
     // or
    /*jdbcDF.write.mode("append").format("csv")
      .save("src/main/resources/csv_output")*/

   /* Default write format is parquet
    jdbcDF.write.mode("append")
      .save("src/main/resources/parquet_format")

      dbcDF.write.mode("append").format("parquet")
      .save("src/main/resources/parquet_output")
    */
    //Another option

//    jdbcDF.write.mode("append").parquet("src/main/resources/parquet1_output")
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","training")
    jdbcDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/ecommerce",
      "salestable",props)
//    jdbcDF.write.mode("append").format("jdbc")  `-> wont work in current version


  }

}
