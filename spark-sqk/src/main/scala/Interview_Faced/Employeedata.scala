package Interview_Faced

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 12/11/18.
  */
object Employeedata {

  def main(args: Array[String]): Unit = {

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

//    val df2 = df1.withColumn("cont",df1.groupBy(col))

//    val df3 = df2.select("organization","employeename","cont").show()
    import  org.apache.spark.sql.functions.count

//    df1.registerTempTable("emp")

    /*val df3 = df.sql("select organization,employeename,count(organization,employeename) as count_total from emp group by organization,employeename")
    df3.show()*/


    val df4=df1.groupBy("organization","employeename").agg(count("*").alias("count"))
    df4.show()

    /*val df5 = df1.withColumn("count",groupBy("organization","employeename").agg(count("*")))
    df5.show()*/

    //val  df5=df1.withColumn("count",df1.groupBy("organization","employeename").count())


  }

}
