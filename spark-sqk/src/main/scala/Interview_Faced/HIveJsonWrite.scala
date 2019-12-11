package Interview_Faced

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 12/12/18.
  */
object HIveJsonWrite {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hiveWriteJson").setMaster("local")
    val sc = new SparkContext(conf)

    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val salesDF = hqlContext.read.format("json")
      .load("/home/hduser/Projects/spark-sql/src/main/resources/sales.json")

    salesDF.printSchema()
    salesDF.show()

    /*salesDF.registerTempTable("salesjson")

    hqlContext.sql("use json")

    hqlContext.sql("create table json123 as select * from salesjson")
*/

     salesDF.write.format("json").saveAsTable("json.json12345")
    println("table loaded sucessfully")

  }

}
