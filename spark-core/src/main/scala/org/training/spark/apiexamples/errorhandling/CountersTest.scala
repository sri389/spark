package org.training.spark.apiexamples.errorhandling

import org.apache.spark.{SparkConf, SparkContext}
import org.training.spark.apiexamples.serialization.SalesRecordParser

/**
  * Created by hduser on 5/27/18.
  */
object CountersTest{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("counters test")

    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile(args(0))

    val missingFieldRecords = sc.accumulator(0)

    val rdd1 = fileRDD.foreach(rec => {
      val parseResult = SalesRecordParser.parse(rec)
      if (parseResult.isLeft) missingFieldRecords += 1
    })

    //println(rdd1.count)
    println(missingFieldRecords.value)

    //Thread.sleep(100000)
  }
}




