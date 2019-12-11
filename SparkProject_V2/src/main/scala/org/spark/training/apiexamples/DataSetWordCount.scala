package org.spark.training.apiexamples

import org.apache.spark.sql.SparkSession

/**
  * Created by hduser on 6/5/16.
  */
object DataSetWordCount {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._

    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    // here .as[string] is encoder  it is build in available


    val words = data.flatMap(value => value.split("\\s+"))

    println("words are")
    words.show()

    //val groupedWords = words.groupByKey(_.toLowerCase)
    val groupedWords = words.groupByKey(_.toLowerCase)

    println("grouped words are")

    println(groupedWords.keys)

    val counts = groupedWords.count()

    counts.show()


  }

}
