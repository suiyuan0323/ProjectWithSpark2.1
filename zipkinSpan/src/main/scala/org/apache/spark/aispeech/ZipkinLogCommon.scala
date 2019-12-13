package org.apache.spark.aispeech

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/11/19 16:52
  * @version 1.0
  */
object ZipkinLogCommon {
  /**
    * 1、清洗log，取出需要的字段，并做过滤
    * 2、过滤service 服务名
    * 3、过滤 kind ：client 和server
    *
    * @param sourceDF log
    * @return id, traceId, kind, logTime, duration, serviceName, remoteIp, tags
    */
  def getZipkinDFFromLogDF(spark: SparkSession,
                           kindClient: String,
                           kindServer: String,
                           sourceDF: DataFrame,
                           tempTableName: String) = {
    import spark.implicits._
    // record有的是array，有的不是，统一组织成array，explode成多行，之后处理
    val messageArray = udf((message: String) => {
      val messageString = if (JSON.isValidArray(message)) message else "[" + message + "]"
      val jsonArray = JSON.parseArray(messageString).iterator()
      var resArray = scala.collection.mutable.ArrayBuffer[String]()
      while (jsonArray.hasNext) {
        val jsonItem = jsonArray.next()
        resArray += jsonItem.toString
      }
      resArray
    })

    val logDF = sourceDF.selectExpr("cast(value as String)")
      .select(explode(messageArray(get_json_object($"value", "$.message"))).as("message"))
      .select(get_json_object($"message", "$.id").alias("id"),
        get_json_object($"message", "$.traceId").alias("traceId"),
        from_unixtime(get_json_object($"message", "$.timestamp").cast("long") / 1000000).cast("timestamp").alias("logTime"),
        get_json_object($"message", "$.kind").alias("kind"),
        get_json_object($"message", "$.duration").alias("duration"),
        get_json_object($"message", "$.localEndpoint.serviceName").alias("serviceName"),
        get_json_object($"message", "$.remoteEndpoint.ipv4").alias("remoteIp"),
        get_json_object($"message", "$.tags").alias("tags"))
      .filter(
        s""" id is not null
           | and traceId is not null
           | and ((kind = '${kindClient}') or (kind = '${kindServer}' and tags is not null))
           | and serviceName is not null
           | and duration is not null
           | and logTime is not null
          """.stripMargin)
      .select(
        $"id", $"traceId", $"kind", $"logTime", $"duration", $"serviceName",
        when($"kind".equalTo(kindClient), "")
          .otherwise(
            when($"traceId".endsWith($"id"), "itself")
              .otherwise(
                when($"remoteIp".isNull, "unknown").otherwise($"remoteIp"))
          ).as("remoteIp"),
        $"tags")
    logDF.createOrReplaceTempView(tempTableName)
  }
}
