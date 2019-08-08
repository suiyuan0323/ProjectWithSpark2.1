package org.apache.spark

import org.apache.spark.aispeech.udf.{DurationConcat, SparkUserUDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 按时间段做聚合
  *
  * @author xiaomei.wang
  * @date 2019/6/13 18:58
  * @version 1.0
  */
case class SpanSummary(spark: SparkSession) {

  import spark.implicits._

  val conf = spark.sparkContext.getConf

  def doAgg(df: DataFrame, aggTime: String) = {

    // 元素之间的连接符
    val idAndDurationContactChar = "&"
    val computePercent = udf((durations: String, index: Int) => {
      if (durations.isEmpty) ""
      else {
        val source = durations.split(idAndDurationContactChar)
        val place = if (index >= source.length) source.length - 1 else if (index < 0) 0 else index
        source(place)
      }
    })

    // 多个idDuration之间连接
    val durationContachChar = ","
    /* UDAF：多列合并成一个list */
    lazy val concatDur = new DurationConcat(durationContachChar)
    val durationChar = ":"
    /* UDAF: 根据id对duration去重，之后返回duration有序list */
    val distinctDuration = udf((messageString: String) => SparkUserUDF.collectDuration(messageString.split(durationContachChar).toList, idAndDurationContactChar, durationChar))

    df.withWatermark("logTime", conf.get("spark.aispeech.data.span.watermark.delay"))
      .groupBy(window($"logTime", aggTime, aggTime), $"serviceName") // serviceName 做group
      .agg(concatDur(concat_ws(idAndDurationContactChar, $"id", $"duration")) as "durationList") // 为了对duration做去重
      .select($"window", $"serviceName", explode(split(distinctDuration($"durationList"), ",")).as("IdDurationList")) // 返回去重后durationList
      .withColumn("splitList", split($"IdDurationList", durationChar))
      .withColumn("durationList", $"splitList".getItem(0))
      .withColumn("query_amount", $"splitList".getItem(1))
      .withColumn("avg_duration", $"splitList".getItem(2))
      .select(md5(concat($"serviceName")).as("id"),
        $"window.start".cast("long").*(1000).as("beginTimestamp"),
        $"window.end".cast("long").*(1000).as("endTimestamp"),
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"),
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"),
        $"serviceName",
        $"query_amount".cast("int"),
        $"avg_duration".cast("int"),
        computePercent($"durationList", $"query_amount".*(0).cast("int")).cast("int").as("min_duration"),
        computePercent($"durationList", $"query_amount".*(1).cast("int")).cast("int").as("max_duration"),
        computePercent($"durationList", $"query_amount".*(0.5).cast("int")).cast("int").as("p50_duration"),
        computePercent($"durationList", $"query_amount".*(0.9).cast("int")).cast("int").as("p90_duration"),
        computePercent($"durationList", $"query_amount".*(0.99).cast("int")).cast("int").as("p99_duration"))
      .createOrReplaceTempView("tempTable")
    val res = spark.sql(
      s"""
         | select id, beginTimestamp, endTimestamp, beginTime, endTime,
         |        serviceName,
         |        "all" as fromService,
         |        "/all" as path,
         |        "all" as method,
         |        query_amount,avg_duration, min_duration, max_duration, p50_duration, p90_duration, p99_duration
         | from tempTable
       """.stripMargin)
    res.printSchema()
    res
  }

  /**
    * 将数据解析成需要的格式
    * traceId, id, serviceName, fromService, duration, path, method, logTime
    *
    * @param df
    * @return
    */
  def parseSpan(df: DataFrame) = {
    df.selectExpr("cast(value as String)")
      .withColumn("span", split(($"value"), ","))
      .select($"span".getItem(0).as("traceId"),
        $"span".getItem(1).as("id"),
        $"span".getItem(2).as("serviceName"),
        $"span".getItem(4).as("duration"),
        $"span".getItem(7).as("logTime").cast("timestamp"))
      .filter(
        """
          | traceId is not null
          | and id is not null
          | and serviceName is not null
          | and duration is not null
          | and logTime is not null
        """.stripMargin)
  }
}
