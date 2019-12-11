package scala_Assignments

/**
  * Created by hduser on 12/13/18.
  */
object Sumof10NUmbersReturned_6 {

  def main(args: Array[String]): Unit = {
    val b = System.in.read()
    val c = b-48
    val a = sum10(c)
    println("sum of 10 numbers are "+a)

  }
  def sum10(x:Int)={
    var z = 0
    for(i<-x to x+9)
      z=z+i
    z
  }

}
