package scala_Assignments

/**
  * Created by hduser on 12/12/18.
  */
object Print10Integers_4 {
  def main(args: Array[String]): Unit = {
println("enter value")
    val j = args(0).toInt

    val k = System.in.read()

    for (i<-j to j+9 by 1)println(i)

  }

}
