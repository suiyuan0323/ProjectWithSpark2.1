package org.apache.spark.aispeech.process

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/11/7 15:51
  * @version 1.0
  */
case class BaserverPvUv(spark: SparkSession) {

  def processDataUv(source: DataFrame) = {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    val queryName = conf.get("spark.aispeech.queryName")

    parseLog(source)
      .groupBy(window($"logTime", conf.get("spark.aispeech.data.interval"), conf.get("spark.aispeech.data.interval")), $"module")
      .agg(approx_count_distinct("sessionId").alias("count"))
      .select($"module",
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
        $"window.start".cast("long").*(1000).as("beginTimestamp"), // long
        $"window.end".cast("long").*(1000).as("endTimestamp"), // long
        $"count")
      .createOrReplaceTempView(queryName + "TempTable")

    val res = spark.sql(
      s""" select "${queryName}" as stats_code,
         |        "${queryName}" as statsItem,
         |        count as resultNum,
         |        '' as resultDetails,
         |        beginTime, endTime, beginTimestamp, endTimestamp
         | from ${queryName}TempTable
       """.stripMargin)
    res
  }

  def processDataPv(source: DataFrame) = {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    val queryName = conf.get("spark.aispeech.queryName")

    parseLog(source)
    /* .groupBy(window($"logTime", conf.get("spark.aispeech.data.interval"), conf.get("spark.aispeech.data.interval")), $"module")
     .count()
     .select(
       $"module",
       date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
       date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
       $"window.start".cast("long").*(1000).as("beginTimestamp"), // long
       $"window.end".cast("long").*(1000).as("endTimestamp"), // long
       $"count")
     .createOrReplaceTempView(queryName + "TempTable")

   val res = spark.sql(
     s""" select "${queryName}" as stats_code,
        |        "${queryName}" as statsItem,
        |        count as resultNum,
        |        '' as resultDetails,
        |        beginTime, endTime, beginTimestamp, endTimestamp
        | from ${queryName}TempTable
      """.stripMargin)

   res*/
  }


  private def parseLog(source: DataFrame) = {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
    source
      .selectExpr("cast(value as String)")
      .select(
        get_json_object($"value", "$.module").as("module"),
        get_json_object($"value", "$.eventName").as("eventName"),
        get_json_object($"value", "$.logTime").as("logTime"),
        when(get_json_object($"value", "$.message.businessType") === ("BA"),
          get_json_object($"value", "$.message.input.externalSessionId"))
          .otherwise(get_json_object($"value", "$.message.input.session")).as("sessionId")
      ).filter(
      s""" module = '${conf.get("spark.aispeech.data.module")}'
         | and eventName = '${conf.get("spark.aispeech.data.eventName")}'
         | and logTime is not null
         | """.stripMargin)
    //  .select($"module", $"logTime".cast("timestamp"), $"sessionId")
    //  .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
    //  and sessionId is not null
  }
}
