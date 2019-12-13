package org.apache.spark.aispeech

import com.alibaba.fastjson.JSON
import org.apache.spark.aispeech.udaf.{BuildSpan, MessageConcat}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/11/19 16:50
  * @version 1.0
  */

case class ZipkinLogSpanProcess(spark: SparkSession)
  extends Logging {

  import spark.implicits._

  val conf = spark.sparkContext.getConf

  def getSpanDF(logDF: DataFrame) = {
    // 内存中临时表的名字
    val tempTableName = "zipkinTempTableUdf"
    val kindClient = "CLIENT"
    val kindServer = "SERVER"

    ZipkinLogCommon.getZipkinDFFromLogDF(spark, kindClient, kindServer, logDF, tempTableName)

    // 2、合成span
    lazy val buildSpan = BuildSpan(kindServer, kindClient)

    spark.udf.register("jsonTag",
      (str: String, key: String) =>
        if (JSON.parseObject(str).get(key) != null)
          JSON.parseObject(str).get(key).toString
        else "")

    val spanDF = spark.sql(
      s"""
         | select logTime, traceId, id, kind, duration, serviceName,
         |        if(kind = '${kindClient}', serviceName, remoteIp) as fromService,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.method'), '') as method,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.path'), '') as path
         | from ${tempTableName}
          """.stripMargin)
      .filter(s"( kind = '${kindClient}' or ( kind = '${kindServer}' and method != '' and path != '' ))")
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(
        window($"logTime",
          conf.get("spark.aispeech.span.window.duration"),
          conf.get("spark.aispeech.span.slide.duration")),
        $"traceId", $"id")
      .agg(
        buildSpan(
          $"kind",
          $"logTime".cast("string"),
          $"duration".cast("long"),
          $"serviceName",
          $"fromService",
          $"method",
          $"path"
        ).as("spanMessage"))
      .withColumn("span", split($"spanMessage", ","))
      .select(
        $"traceId",
        $"id",
        $"span".getItem(2).as("serviceName"),
        $"span".getItem(3).as("fromService"),
        $"span".getItem(1).cast("long").as("duration"),
        $"span".getItem(5).as("path"),
        $"span".getItem(4).as("method"),
        date_format($"span".getItem(0).cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("logTime")
      ).filter( """ serviceName != '' """)

    spanDF
  }
}
