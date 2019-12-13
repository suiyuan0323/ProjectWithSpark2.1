package org.apache.spark.aispeech.process

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/10/24 10:28
  * @version 1.0
  */
case class SourcePv(spark: SparkSession) {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  def processData(source: DataFrame) = {
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
        when($"businessType".equalTo("BA"), lower($"businessType"))
          .otherwise(get_json_object($"message", "$.message.input.context.source.platform")).as("source"))
      .filter("source is not null")
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(window($"logTime", conf.get("spark.aispeech.data.interval"), conf.get("spark.aispeech.data.interval")), $"source")
      .count()
      .select(
        concat(lit(s"${queryName}@"), $"source").as("source"),
        $"count",
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
        $"window.start".cast("long").*(1000).as("beginTimestamp"), // long
        $"window.end".cast("long").*(1000).as("endTimestamp")) // long
      .withColumn("prefix", lit(s"${queryName}"))
      .createOrReplaceTempView(queryName + "TempTable")

    val res = spark.sql(
      s""" select "${queryName}" as stats_code,
         |        source as statsItem,
         |        count as resultNum,
         |        '' as resultDetails,
         |        beginTime, endTime, beginTimestamp, endTimestamp
         | from ${queryName}TempTable
       """.stripMargin)
    res
  }
}
