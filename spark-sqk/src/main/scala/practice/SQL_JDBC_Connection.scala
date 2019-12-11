package practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
  * Created by hduser on 8/30/18.
  */
object SQL_JDBC_Connection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SqlRead")

    val sc = new SparkContext(conf)

    val sqlContext =new SQLContext(sc)

    val sqlProps = Map(
      "url"->"jdbc:mysql://localhost:3306/ecommerce",
      "dbtable"->"sales",
      "user"->"root",
      "password"->"training"
      )

    val sqlDF = sqlContext.read.format("jdbc").options(sqlProps).load()

    sqlDF.printSchema()
    sqlDF.show()
    print(sqlDF.rdd.getNumPartitions)



  }

}
