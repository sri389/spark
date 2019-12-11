package scala_Assignments

/**
  * Created by hduser on 12/12/18.
  */
object Sums10Numbers_5 {

  def main(args: Array[String]): Unit = {

    val z:Int = System.in.read()
    z.toInt
    println(z)
    val x = z-48
    println(sumof10(x))

    def sumof10(x:Int)={
      var z = 0
      for (i<-x to x+9)
        z=z+i
      z
    }

  }

}
