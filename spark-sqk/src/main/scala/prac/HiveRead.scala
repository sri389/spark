package prac

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 8/31/18.
  */
object HiveRead {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Spark_Hive")
    val sc = new SparkContext(conf)
    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    // these properties taken from hive-site.xml

    val hc = new HiveContext(sc)
    //    val sqlContext = new SQLContext(hc)
    val csvDF = hc.read.format("csv").option("inferSchema","true").option("header","true")
      .load("src/main/resources/sales.csv")

    csvDF.show()
    csvDF.printSchema()
    csvDF.registerTempTable("test")




    //step - 1
//    csvDF.write.format("csv").mode("append").saveAsTable("training.xyz789")

    //step-2
    //csvDF.write.insertInto("training.xyz789")

    //step-3 overwrite
    hc.sql("use training")
//    hc.sql("insert overwrite table xyz789 select * from test")
hc.sql("create table xyz456 as select * from test")
    println("table loaded sucessfully")
    hc.sql("select count(*) from training.xyz456").show()







  }

}
