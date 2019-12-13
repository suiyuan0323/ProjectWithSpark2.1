package org.apache.spark.logError

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/6/30 14:43
  * @version 1.0
  */
case class ErrorLogProcess(spark: SparkSession) extends Logging {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  /**
    * 按照给定时间间隔做统计，统计字段columns
    *
    * @param intervalTime
    * @param columns
    */
  def statsByTimeWindow(dataFrame: DataFrame) = {
    // 解析log
    parseLog(dataFrame).createOrReplaceTempView("sparkTable")
    // stats
    spark.sql(
      s""" select id,
         |        module,
         |        1 as log_error,
         |        if(instr(exception_message, '500')>0, 1, 0) as log_500,
         |        if(instr(exception_message, '400')>0, 1, 0) as log_400,
         |        logTime
         | from sparkTable """.stripMargin)
      // 后期 对id去一下重复。重复消费
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(
        window($"logTime", conf.get("spark.aispeech.data.agg.duration.time"), conf.get("spark.aispeech.data.agg.duration.time")),
        $"module")
      .agg(
        sum($"log_error").as("error_count"),
        sum($"log_500").as("500_count"),
        sum($"log_400").as("400_count"))
      .select(
        $"module",
        $"window.start".cast("long").*(1000).as("beginTimestamp"),
        $"window.end".cast("long").*(1000).as("endTimestamp"),
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"),
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"),
        $"error_count",
        $"500_count",
        $"400_count"
      )
  }

  /**
    * 解析log
    *
    * @param df
    * @return
    */
  private def parseLog(df: DataFrame) = {
    df.selectExpr("cast(value as String)")
      .filter(lower(trim(get_json_object($"value", "$.level"))).equalTo("error"))
      .select(
        md5($"value").as("id"),
        get_json_object($"value", "$.module").as("module"),
        get_json_object($"value", "$.exception.message").as("exception_message"),
        regexp_replace(get_json_object($"value", "$.logTime"), "T", " ")
          .as("logTime").cast("timestamp")
      )
  }
}
