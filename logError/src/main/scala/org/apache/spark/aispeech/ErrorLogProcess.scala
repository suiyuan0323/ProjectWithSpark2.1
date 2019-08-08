package org.apache.spark.aispeech

import org.apache.spark.aispeech.utils.{ColumnConcat, UserFun}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/6/30 14:43
  * @version 1.0
  */
case class ErrorLogProcess(spark: SparkSession, dataFrame: DataFrame) extends Logging {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  /**
    * 按照给定时间间隔做统计，统计字段columns
    *
    * @param intervalTime
    * @param columns
    */
  def satisticsByTime(intervalTime: String, columns: List[String]) = {
    parseLog(dataFrame).createOrReplaceTempView("sparkTable")
    /* UDAF：多列合并成一个list */
    lazy val concatDur = new ColumnConcat(",")
    // 元素之间的连接符
    val contactChar = "&"
    val staticLog = udf((collectMessageList: String) => UserFun.staticByWindow(collectMessageList.split(",").toList, contactChar))
    spark.sql(
      s""" select id,
         |        module,
         |        1 as log_error,
         |        if(instr(exception_message, '500')>0, 1, 0) as log_500,
         |        if(instr(exception_message, '400')>0, 1, 0) as log_400,
         |        logTime
         | from sparkTable """.stripMargin)
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(
        window($"logTime", conf.get("spark.aispeech.data.agg.duration.time"), conf.get("spark.aispeech.data.agg.duration.time")),
        $"module")
      .agg(concatDur(concat_ws(contactChar, $"id", $"log_error", $"log_500", $"log_400")) as "messageList")
      .withColumn("count_list", split(staticLog($"messageList"), ","))
      .select(
        $"module",
        $"window.start".cast("long").*(1000).as("beginTimestamp"),
        $"window.end".cast("long").*(1000).as("endTimestamp"),
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"),
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"),
        $"count_list".getItem(0).as("error_count"),
        $"count_list".getItem(1).as("500_count"),
        $"count_list".getItem(2).as("400_count")
      )
  }

  private def parseLog(df: DataFrame) = {
    df.selectExpr("cast(value as String)")
      .withColumn("id", md5($"value"))
      .withColumn("level", get_json_object($"value", "$.level"))
      .filter(s"""  level = 'error' """)
      .select(
        $"id",
        get_json_object($"value", "$.module").as("module"),
        get_json_object($"value", "$.exception.message").as("exception_message"),
        regexp_replace(get_json_object($"value", "$.logTime"), "T", " ").as("logTime").cast("timestamp")
      )
  }
}
