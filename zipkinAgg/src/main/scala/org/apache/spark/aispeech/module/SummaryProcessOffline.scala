package org.apache.spark.aispeech.module

import org.apache.spark.aispeech.udf.PercentileApprox
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/11/12 17:38
  * @version 1.0
  */
case class SummaryProcessOffline(spark: SparkSession) {

  import spark.implicits._

  val conf = spark.sparkContext.getConf

  /**
    * 离线
    *
    * @param spanDF
    * @return
    */
  def getSummarySQL(spanDF: DataFrame, beginSecondTimestamp: Int, endSecondTimestamp: Int): DataFrame = {
    val summary = spanDF
      .dropDuplicates("traceId", "id")
      .groupBy($"serviceName", $"fromService", $"path", $"method")
      .agg(
        count($"id").as("query_amount"),
        sum($"duration").as("count_duration"),
        min($"duration").as("min_duration"),
        max($"duration").as("max_duration"),
        PercentileApprox.percentile_approx($"duration", lit(Array(0.5, 0.9, 0.99))).as("PercentDurations")
      )
      .withColumn("beginTime", lit(beginSecondTimestamp).cast("timestamp"))
      .withColumn("endTime", lit(endSecondTimestamp).cast("timestamp"))

    summaryDataFrameFormat(summary)
  }

  /**
    * 实时
    *
    * @param spanDF
    * @return
    */
  def getSummaryStreaming(spanDF: DataFrame): DataFrame = {
    val summary = spanDF
      .dropDuplicates("traceId", "id")
      .groupBy(
        window($"logTime",
          conf.get("spark.aispeech.summary.agg.window.duration"),
          conf.get("spark.aispeech.summary.agg.slide.duration")),
        $"serviceName", $"fromService", $"method", $"path")
      .agg(
        count($"id").as("query_amount"),
        sum($"duration").as("count_duration"),
        min($"duration").as("min_duration"),
        max($"duration").as("max_duration"),
        PercentileApprox.percentile_approx($"duration", lit(Array(0.5, 0.9, 0.99))).as("PercentDurations")
      )
      .withColumn("beginTime", $"window.start".cast("timestamp"))
      .withColumn("endTime", $"window.end".cast("timestamp"))

    summaryDataFrameFormat(summary)
  }

  private def summaryDataFrameFormat(summaryDataFrame: DataFrame) = {
    summaryDataFrame
      .select(
        md5(concat($"beginTime", $"endTime", $"serviceName", $"fromService", $"method", $"path")).as("id"),
        //        from_unixtime($"window.start".cast("long")).as("beginTime"),
        //        from_unixtime($"window.end".cast("long")).as("endTime"),
        date_format($"beginTime", "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
        date_format($"endTime", "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
        $"serviceName",
        $"fromService",
        $"method",
        $"path",
        $"min_duration",
        $"max_duration",
        ($"count_duration" / $"query_amount").as("avg_duration").cast("long"),
        $"query_amount",
        $"PercentDurations" (0).as("p50_duration"),
        $"PercentDurations" (1).as("p90_duration"),
        $"PercentDurations" (2).as("p99_duration")
      )
  }
}
