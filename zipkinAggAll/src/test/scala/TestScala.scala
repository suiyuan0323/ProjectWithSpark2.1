import org.apache.spark.aispeech.udf.SparkUserUDF

/**
  * @author xiaomei.wang
  * @date 2019/7/11 18:49
  * @version 1.0
  */
object TestScala {
  def main(args: Array[String]): Unit = {
    val messageList = List("a-123", "a-123", "b-34")
    val contactChar = "-"
    val res = SparkUserUDF.collectDuration(messageList, contactChar, ":")
    println(res)
  }
}
