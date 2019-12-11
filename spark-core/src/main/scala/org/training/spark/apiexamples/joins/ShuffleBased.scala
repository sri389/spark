package org.training.spark.apiexamples.joins

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by Arjun on 20/1/15.
 */
object ShuffleBased {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)

    val salesRDD = sc.textFile(args(1),3)
    val customerRDD = sc.textFile(args(2),2)

    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId,salesRecord)
    })

    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })


    val joinRDD = customerPair.join(salesPair)
    joinRDD.foreach(println)
      val result = joinRDD.map{
      case (customerId,(customerName,salesRecord)) => {
        (customerName,salesRecord.itemId)
      }
    }

    /*val result = joinRDD.map{record => {

        val customerId = record._1
        val customerName = record._2._1
        val itemId = record._2._2.itemId
        (customerName,itemId)
      }
    }*/

    println(result.collect().toList) // Not recommended to use colllect after join....

    val joinRDD1 = salesPair.join(customerPair)

    val result1 = joinRDD1.map{
      case (customerId,(salesRecord,customerName)) => {
        (customerName,salesRecord.itemId)
      }
    }

    println(result1.collect().toList)

    Thread.sleep(50000)

  }


}
