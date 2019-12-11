package org.training.spark.testing

/**
  * Created by hduser on 7/2/17.
  */
import java.io.File

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveMetaDataClient extends App {

  val hconf = new HiveConf

  val f = new File("/home/hduser/apache-hive-1.2.1-bin/conf/hive-site.xml")
  if(f.exists()) {
    println("file exists")
    hconf.addResource(f.toURI.toURL)
  }

  /*conf.set("hive.metastore.warehouse.dir", "file://tmp/hive/new")
  conf.set("hive.metastore.local", "true")
  conf.set("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=/IBM_Training/Spark/Spark_Training/SparkProject_1/metastore_db;create=true")
  conf.set("javax.jdo.option.ConnectionDriverName","org.apache.derby.jdbc.EmbeddedDriver")
  */
  //conf.setVar(HiveConf.ConfVars.METASTOREURIS, "C:\\IBM_Training\\Spark\\Spark_Training\\SparkProject_1\\metastore_db")
  println(HiveConf.getVar(hconf, HiveConf.ConfVars.METASTOREURIS))

  val client = new HiveMetaStoreClient(hconf)
  val dbs = client.getAllDatabases.toArray

  for(db <- dbs) {
    val tables = client.getAllTables(db.toString).toArray
    println(tables.foreach(println))
  }

  val table = client.getTable("default","sales")
  val sd = table.getSd
  val storageLocation = sd.getLocation
  println(storageLocation)

  val partitions = client.listPartitions("spark","itemid_partitions",100).toArray
  partitions.foreach(println)

  val fields = client.getSchema("spark", "itemid_partitions").toArray
  fields.foreach(println)

  val sconf = new SparkConf().setAppName("hive metastore app").setMaster("local")
  val sc = new SparkContext(sconf)
  val sqlContext = new SQLContext(sc)

  val salesDF = sqlContext.read.parquet(storageLocation)

  salesDF.show()
}