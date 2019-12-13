package org.apache.spark.kfParallelism

import org.apache.spark.sql.functions.{concat_ws, date_format, get_json_object, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/11/19 14:30
  * @version 1.0
  */
case class KfParallelismProcess(spark: SparkSession) {

  lazy val conf = spark.sparkContext.getConf

  def statsByTimeWindow(kafkaSource: DataFrame) = {
    import spark.implicits._

    lazy val onlineStats = new OnlineAggUdf(
      conf.get("spark.aispeech.redis.address"),
      conf.get("spark.aispeech.redis.password"),
      conf.get("spark.aispeech.redis.key.prefix"),
      redisKeyTimeOutMax = conf.getInt("spark.aispeech.redis.key.timeOut.max", 1800),
      redisKeyTimeOutMin = conf.getInt("spark.aispeech.redis.key.timeOut.min", 60),
      redisReplicas = conf.getInt("spark.aispeech.redis.key.replicas", 3),
      redisReplicasTimeoutMilliseconds = conf.getInt("spark.aispeech.redis.key.replicas.timeout.millisecond", 2),
      callOnOperator = conf.get("spark.aispeech.data.eventName.callOn"),
      callEndOperator = conf.get("spark.aispeech.data.eventName.callEnd"))

    parseLog(kafkaSource)
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(
        window($"logTime", conf.get("spark.aispeech.data.interval"), conf.get("spark.aispeech.data.interval")),
        $"module", $"productId")
      .agg(
        onlineStats(
          concat_ws("-", $"module", $"productId"),
          $"sessionId",
          $"eventName")
          .cast("int").as("onlineNum"))
      .select(
        $"module",
        $"productId",
        $"onlineNum",
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("time") // time
      )
  }

  private def parseLog(kafkaSource: DataFrame) = {
    import spark.implicits._
    kafkaSource.selectExpr("cast(value as String)") // key is null
      .select(
      get_json_object($"value", "$.module").as("module"),
      get_json_object($"value", "$.eventName").as("eventName"),
      get_json_object($"value", "$.sessionId").as("sessionId"),
      get_json_object($"value", "$.productId").as("productId"),
      get_json_object($"value", "$.logTime").as("logTime"))
      .filter(
        $"eventName".isin(conf.get("spark.aispeech.data.eventName").split(",").toSeq: _*)
          and $"module".isin(conf.get("spark.aispeech.data.module").split(",").toSeq: _*)
          and $"productId".isNotNull
          and $"logTime".isNotNull
          and $"sessionId".isNotNull)
      .select($"module", $"eventName", $"sessionId", $"productId", $"logTime".cast("timestamp"))
  }
}
