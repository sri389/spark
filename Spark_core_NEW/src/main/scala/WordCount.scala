import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 9/27/18.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val textRdd = sc.textFile(" file:///home/hduser/input_files/sample.txt")

    val words = textRdd.flatMap(x=>x.split(" "))

    val keyvaluepair = words.map(x=>(x,1))

//    val grouping = keyvaluepair.groupByKey()

    val wordCount = keyvaluepair.reduceByKey(_ + _)

    wordCount.foreach(println)
//    textRdd.collect()
//    libraryDependencies+= "org.apache.spark" %% "spark-core" % "1.6.1"

  }
}
