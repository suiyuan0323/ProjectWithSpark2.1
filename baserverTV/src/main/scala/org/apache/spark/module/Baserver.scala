package org.apache.spark.module

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/8/7 11:00
  * @version 1.0
  */
case class Baserver(spark: SparkSession, module: String = "baserver", eventName: String = "sys_input_output", sourceDF: DataFrame) extends Logging {

  def statisticsByTime(moduleDF: DataFrame) = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    val timeInterval = conf.get("spark.aispeech.data.interval")
    moduleDF.withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      //      .dropDuplicates("sessionId") // 去重
      .groupBy(window($"logTime", timeInterval, timeInterval), $"module", $"eventName"/*, $"sessionId"*/)
      .count()
      .select($"count")
  }

  /**
    * 过滤出需要统计的module和eventName
    *
    * @return
    */
  def initModuleSource() = {
    import spark.implicits._
    val res = sourceDF.selectExpr("cast(value as String)") // key is null
      .select(get_json_object($"value", "$.module").alias("module"), // 过滤module
      get_json_object($"value", "$.eventName").alias("eventName"), // 过滤
      get_json_object(get_json_object(get_json_object(gt_json_object($"value", "$.message"), "$.message"), "$.input"), "$.externalSessionId").alias("sessionId"), // distinct
      get_json_object($"value", "$.logTime").alias("logTime").cast("timestamp")) //每分钟
      .filter(
      s""" module = '${module}'
         | and eventName = '${eventName}'
         | and sessionId is not null
         | and logTime is not null
          """.stripMargin)
    res.printSchema()
    res
  }
}
