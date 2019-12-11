package scala_Assignments

import scala.io.Source

/**
  * Created by hduser on 12/12/18.
  */
object WordCount {

  def main(args:Array[String]):Unit={
    val file = Source.fromFile("/home/hduser/Desktop/spark-sql/test.txt").getLines().toList

    val lines = file.flatMap(x=>x.split(" "))

    val grouping = lines.groupBy(x=>x)

    val wordCount = grouping.map(x=>(x._1,x._2.size))

    wordCount.foreach(println)

println("_________")
    val wc = wordCount.toList.sortBy(x=> -x._2)


    wc.foreach(println)
println("_____________")
    val wc1 = wc.map(x=>x._1)
    wc1.take(3).foreach(println)


 }

}
