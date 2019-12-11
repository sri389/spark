package Interview_Faced

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, functions}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 12/12/18.
  */
object EmployeData {

  def main(args:Array[String]):Unit={


    val conf = new SparkConf().setAppName("d").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)
    val hc=new HiveContext(sc)

    val df = sqlcontext.read.format("csv").option("inferschema","true").option("header","true")
      .load("/home/hduser/Desktop/data")

    df.printSchema()
    df.show()

    val df1 = df.select("organization","employeename")
    df1.show()
    import  org.apache.spark.sql.functions._

    val df2 = df1.groupBy("organization", "employeename").agg(count("*").as("Count"))
    df2.show()

    df1.registerTempTable("emp")

    sqlcontext.sql("select organization,employeename,count(organization,employeename) as Count from emp group by organization,employeename").show()

  }

}
