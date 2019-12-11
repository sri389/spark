package Interview_Faced

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 12/12/18.
  */
object NMaxSalary {

  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setAppName("NamxSAlary").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    val df = sqlcontext.read.format("csv").option("inferschema","true").option("header","true")
      .load("/home/hduser/Projects/spark-sql/src/main/resources/NmaxSalaryData")

    df.printSchema()
    df.show()

    df.registerTempTable("emp")

    sqlcontext.sql("select * from (select Id,Name,Salary,DeptId,dense_rank() over(partition by DeptId order by Salary desc)rn from emp)x where x.rn = 2").show()

  }

}
