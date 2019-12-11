package learning
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
/**
  * Created by hduser on 9/1/18.
  */
object SparkUdf1 {
  def getDiscountedPrice(price:Double)={
    price * 0.9
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("UdfCreation")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val optionsMap = Map("Inferschema"->"true","header"->"true")
    val csvDf = sqlContext.read.format("csv").options(optionsMap).load("src/main/resources/sales.csv")

    /*val calcDiscountedPrice = udf((price:Double)=>{
      price * 0.9
    })*/

    val calcDiscountedPrice=udf(getDiscountedPrice _)

    val finalDf = csvDf.withColumn("discountedAmount",
      calcDiscountedPrice(col("amountPaid")))

    finalDf.printSchema()
    finalDf.show()

    sqlContext.udf.register("computediscount",(price:Double)=>{price*0.9})

    csvDf.registerTempTable("test")

    val sqlDf = sqlContext.sql("select *, computediscount(amountPaid) from test")
    sqlDf.printSchema()
    sqlDf.show()
  }
}