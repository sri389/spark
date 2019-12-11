package Interview_Faced

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 11/18/18.
  */
object AddingColumnsTotal {

  def main(args: Array [String]):Unit = {

    val conf = new SparkConf().setAppName("Adding Columns").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    val df = sqlcontext.read.format("csv").option("header","true").option("inferschema","true").
      load("/home/hduser/Projects/spark-sql/src/main/resources/Interview_Sample_Data/addingcolumns")

    df.show()

    val df2 = df.withColumn("sumsal",df("2013")+df("2014")+df("2015")+df("2016")).show()


    df.withColumn("2013",df("2013")+5).show()

    //df.selectExpr("empid",col("2013")+5 as sal1).show()
    //df.select("empid",df("2013")+5.as("sal")).show()

  }

}
