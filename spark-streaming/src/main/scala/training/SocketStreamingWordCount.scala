package training

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hduser on 9/4/18.
  */
object SocketStreamingWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Xocket_Stream_WordCOunt").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10))

    val lines = ssc.socketTextStream("localhost",50050)



    lines.print()

    ssc.start()

    ssc.awaitTermination()


  }


}
