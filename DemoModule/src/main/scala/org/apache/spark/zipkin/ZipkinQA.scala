package org.apache.spark.zipkin

import org.apache.spark.common.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/9/1 12:53
  * @version 1.0
  */
case class ZipkinQA(spark: SparkSession) extends Logging {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  def processLog(source: DataFrame) = {

    val moduleStream = source.filter(instr($"value", "qa") > 0)
      .withColumn("span", split(($"value"), ","))
      .select($"span".getItem(0).as("traceId"),
        $"span".getItem(1).as("id"),
        $"span".getItem(2).as("serviceName"),
        $"span".getItem(3).as("fromService"),
        $"span".getItem(4).as("duration"),
        $"span".getItem(5).as("path"),
        $"span".getItem(6).as("method"),
        $"span".getItem(7).as("logTime").cast("timestamp"))
    //.select("serviceName")

    val productStream = moduleStream // .filter("serviceName = qa")
    productStream
  }
}
