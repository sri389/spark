package learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 8/31/18.
  */
object SparkHive {

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
//    csvDF.write.mode("append").saveAsTable("training.sparktest1")  // ********
    //        ANother Method1
//    csvDF.write.insertInto("training.sparktest1")     //***************
    //insert into does not create table if not exists
    //used of existing table

    //Another method 2
    hc.sql("use training")
//    hc.sql("insert into sparktest1 select * from test")    //****************
//it will append

//    hc.sql("insert overwrite table sparktest1 select * from test")

    //creation of table with data and schema

    hc.sql("create table sparktest2 as select * from test") //****************
    println("sucessfully table created in hive")


    hc.sql("select count(*) from training.sparktest2").show()

//    println("sucessfully table created in hive")




  }

}
