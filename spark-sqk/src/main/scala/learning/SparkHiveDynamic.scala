package learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 9/1/18.
  */
object SparkHiveDynamic {

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


    //hive dynamic partion true

    hc.sql("create table if not exists training.itemid_partitioned "+
    "(transactionid int,customerid int,amountpaid double)partitioned by (itemid int)")

    hc.setConf("hive.exec.dynamic.partition","true")
    hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")

   //step -1

   // csvDF.registerTempTable("salesdata")
   /*
    hc.sql("insert into training.itemid_partitioned partition(itemid) "+
    "select transactionid,customerid,amountpaid,itemid from salesdata")
    */
    // here partioned should be always at last

    //step-2 inserting
val renamedDf = csvDF.selectExpr(
  "transactionId as transactionid", "customerId as customerid", "amountPaid as amountpaid", "itemId as itemid")
    renamedDf.printSchema()

    renamedDf.write.partitionBy("itemid").insertInto("training.itemid_partitioned")
    // dataframe bothers about case

  }

}
