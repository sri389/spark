import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 8/20/18.
  */
object MoviesAssignment1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Movies_Assignment")

    val context = new SparkContext(conf)

    val movies = context.textFile(args(0)).map(x=>(x.split("::")(0),x.split("::")(1)))

    val ratings = context.textFile(args(1))

    movies.take(10).foreach(println)

    val ratingrecords = ratings.map(x=>x.split("::").toList)//map(x=>x.split)

    ratingrecords.take(10).foreach(println)

    val ratingRDD=ratingrecords.map(x=> (x(1),x(2)))
                          .groupByKey()
                          .map(x=>(x._1,x._2.size))

movies
    ratingRDD.take(10).foreach(println)

    val joinsRdd= ratingRDD.join(movies).map(x=>(x._2._2,x._2._1)).sortBy(x=>x._2,false)

    joinsRdd.take(10).foreach(println)
  }

}
