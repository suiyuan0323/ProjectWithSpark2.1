package org.apache.spark.aispeech.process

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/10/24 11:18
  * @version 1.0
  */
case class PidPv(spark: SparkSession) {

  /**
    * 按照pid统计请求量
    *
    * @param source
    * @return
    */
  def processData(source: DataFrame) = {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    val queryName = conf.get("spark.aispeech.queryName")

    source.selectExpr("cast(value as String)")
      .select(
        get_json_object($"value", "$.message").as("message"),
        get_json_object($"value", "$.module").as("module"),
        get_json_object($"value", "$.eventName").as("eventName"),
        get_json_object($"value", "$.logTime").as("logTime"))
      .filter(
        s""" module = '${conf.get("spark.aispeech.data.module")}'
           | and eventName = '${conf.get("spark.aispeech.data.eventName")}'
           | and logTime is not null
           | and message is not null """.stripMargin)
      .withColumn("businessType", get_json_object($"message", "$.businessType"))
      .select(
        $"logTime".cast("timestamp"),
        when($"businessType".equalTo("BA"), get_json_object($"message", "$.message.input.product.id"))
          .otherwise(get_json_object($"message", "$.message.input.context.product.productId")).as("productId"))
      .filter("productId is not null")
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(window($"logTime", conf.get("spark.aispeech.data.interval"), conf.get("spark.aispeech.data.interval")), $"productId")
      .count()
      .select(
        concat(lit(s"${queryName}@"), $"productId").as("productId"),
        $"count",
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
        $"window.start".cast("long").*(1000).as("beginTimestamp"), // long
        $"window.end".cast("long").*(1000).as("endTimestamp")) // long
      .createOrReplaceTempView(queryName + "TempTable")

    val res = spark.sql(
      s""" select "${queryName}" as stats_code,
         |        productId as statsItem,
         |        count as resultNum,
         |        '' as resultDetails,
         |        beginTime, endTime, beginTimestamp, endTimestamp
         | from ${queryName}TempTable
       """.stripMargin)

    res
  }
}
