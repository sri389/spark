package scala_Assignments

/**
  * Created by hduser on 12/13/18.
  */
object ListOutput_10 extends App {

  //  def main(args: Array[String]): Unit = {
  var l = List(1, 2, 3, 4, 5)
  println(l)

  var x = for {
    e <- l
    abc = e + 1
    xyz = abc - 1
    xyz
  }
    println(x)
}



