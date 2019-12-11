package Interview_Faced

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**
  * Created by hduser on 11/21/18.
  */
object DfTo2Df {

  def main(args:Array[String]):Unit={
    val conf= new SparkConf().setAppName("even_oddDFtoDF").setMaster("local")
    val sc1= new SparkContext(conf)
    val sqlContext= new SQLContext(sc1)
    val df = sqlContext.read.format("csv").option("header","true").option("inferschema","true").
      load("/home/hduser/Projects/spark-sql/src/main/resources/Interview_Sample_Data/EmployeeId")

    df.show()

    val df1_Even = df.filter("Id%2=0").show()
    val df2_Odd = df.filter("Id%2!=0").show()



  }
}

