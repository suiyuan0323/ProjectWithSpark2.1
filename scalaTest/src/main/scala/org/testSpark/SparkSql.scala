package org.testSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/12/12 17:35
  * @version 1.0
  */
object SparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df = spark.createDataFrame((1 to 59).map(i => Record(i, s"2019-12-12 12:1:$i")))
    df.createOrReplaceTempView("tem")
    spark.sql("select key, value, cast(value as timestamp) from tem")
      .show(false)
    spark.sql(" select value, cast(value as timestamp) as time from tem ")
      .withColumn("hour", hour($"time"))
      .show(false)
  }
}

case class Record(key: Int, value: String)
