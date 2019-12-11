package learningWeekendBatch

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions._

/**
  * Created by hduser on 9/15/18.
  */
object ReadJsonFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("ReadJsonFile")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val salesDF = sqlContext.read.format("json").load("/home/hduser/Projects/spark-sql/src/main/resources/sales.json")

//    Another Method
//    sqlContext.read.json("/home/hduser/Projects/spark-sql/src/main/resources/sales.json")

    salesDF.printSchema()
    salesDF.show()

    val subsalesDf = salesDF.select("customerId","amountPaid")

    subsalesDf.printSchema()
    subsalesDf.show()
    val dslDfavg = salesDF.groupBy("customerId").agg(sum("amountPaid").as("total"),avg("amountPaid").as("average"))
    dslDfavg.show()

    dslDfavg.coalesce(1).write.json("/home/hduser/Projects/spark-sql/src/main/resources/jsonfolder")
    //here by default 200 partions will be created

  }

}
