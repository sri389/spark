package scala_Assignments

/**
  * Created by hduser on 12/12/18.
  */
object ProgramInvoke {

  def main(args: Array[String]): Unit = {


    even(10)

  }

  def even(x: Int) = {
    for (i <- 2 to x by 2) println(i)

  }

  def even1(x: Int) = {
    for (i <- 1 to x) if (i % 2 == 0) println(i)
  }
}
