package org.apache.spark

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.aispeech.udaf.MessageConcat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{from_unixtime, get_json_object, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 清洗log，获取span相关信息
  *
  * @author xiaomei.wang
  * @date 2019/6/13 18:35
  * @version 1.0
  */
case class ZipkinLogUtils(spark: SparkSession, kindServer: String, kindClient: String) extends Logging {

  import spark.implicits._

  val conf = spark.sparkContext.getConf

  /**
    * 返回span
    *
    * @return ：kafka的message，string（traceId, id, serviceName, fromService, duration,path, method, logTime）
    */
  def getSpanDF(logDF: DataFrame) = {

    val messageSplitChar = "  &  "
    // udf：合并聚合中的column
    lazy val messageConcat = new MessageConcat(messageSplitChar)
    // udf：合并message为span
    val getSpanMessage = udf((isSelf: Boolean, messageList: String) => combineMessageToSpan(isSelf, messageList.split(messageSplitChar).toList))
    // udf: 取出json中指定的key(这里用于取出method、path)
    spark.udf.register("jsonTag", (str: String, key: String) => if (JSON.parseObject(str).get(key) != null) JSON.parseObject(str).get(key).toString else "")
    // 内存中临时表的名字
    val tempTableName = "zipkinTempTable"
    // 1、清洗log
    getZipkinDFFromLogDF(logDF, tempTableName)
    // 2、合成span
    spark.sql(
      s"""
         | select id, traceId, kind, duration, serviceName, logTime,
         |        if(kind = '${kindServer}', ifnull(remoteIp, 'unknown'), '') as remoteIp,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.method'), '') as method,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.path'), '') as path
         | from ${tempTableName}
         """.stripMargin)
      .withColumn("message", concat_ws("|", $"kind", $"logTime", $"duration", $"serviceName", $"remoteIp", $"method", $"path"))
      .select($"logTime", $"traceId", $"id", $"message")
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(window($"logTime", conf.get("spark.aispeech.span.window.duration"), conf.get("spark.aispeech.span.slide.duration")), $"traceId", $"id")
      .agg(messageConcat(col("message")) as "messageList")
      .withColumn("span", split(getSpanMessage($"traceId".endsWith($"id"), $"messageList"), ","))
      .select($"traceId", $"id",
        $"span".getItem(2).as("serviceName"),
        $"span".getItem(3).as("fromService"),
        $"span".getItem(1).as("duration"),
        $"span".getItem(5).as("path"),
        $"span".getItem(4).as("method"),
        $"span".getItem(0).as("logTime"))
      .filter(s""" serviceName is not null and serviceName != '' and logTime is not null """.stripMargin)
  }

  /**
    * 1、清洗log，取出需要的字段，并做过滤
    * 2、过滤service 服务名
    * 3、过滤 kind ：client 和server
    *
    * @param sourceDF log
    * @return id, traceId, kind, logTime, duration, serviceName, remoteIp, tags
    */
  private def getZipkinDFFromLogDF(sourceDF: DataFrame, tempTableName: String) = {
    val services = spark.sparkContext.getConf.get("spark.aispeech.data.exclude.services")
    logError("log services: " + services)
    val filterServer = udf((serverStr: String) => if (services.split(",").contains(serverStr)) "" else serverStr)
    sourceDF.selectExpr("cast(value as String)") // key is null
      .select(get_json_object($"value", "$.message.id").alias("id"), // 都取出来，没有就是null
      get_json_object($"value", "$.message.traceId").alias("traceId"),
      get_json_object($"value", "$.message.timestamp").cast("float").alias("logTimeStr"),
      get_json_object($"value", "$.message.kind").alias("kind"),
      get_json_object($"value", "$.message.duration").alias("duration"),
      get_json_object($"value", "$.message.localEndpoint.serviceName").alias("serviceName"),
      get_json_object($"value", "$.message.remoteEndpoint.ipv4").alias("remoteIp"),
      get_json_object($"value", "$.message.tags").alias("tags"))
      .withColumn("logTime", from_unixtime($"logTimeStr" / 1000000).cast("timestamp"))
      .filter(
        s""" id is not null
           | and traceId is not null
           | and (kind = '${kindServer}' or kind = '${kindClient}')
           | and serviceName is not null
           | and duration is not null
           | and logTime is not null
          """.stripMargin)
      .withColumn("isFilter", filterServer($"serviceName"))
      .filter($"isFilter".=!=(""))
      .select($"id", $"traceId", $"kind", $"logTime", $"duration", $"serviceName", $"remoteIp", $"tags")
      .createOrReplaceTempView(tempTableName)
  }

  /**
    * 合并traceId和Id相同的message => span， message格式: 以下不能有null或者空字符串，在之前应该做校验。
    * (server):$"kind", $"logTime", $"duration", $"serviceName", $"remoteIp", $"method", $"path"
    * (client):$"kind", $"logTime", $"duration", $"serviceName"
    *
    * @param messageList
    * @return (logTime, duration, serviceName, fromService, method, path)
    */
  private def combineMessageToSpan(isSelf: Boolean, messageList: List[String]): String = {
    var (logTime, duration, serviceName, fromService, method, path) = ("", "0", "", "", "", "")

    def getSpan(message: String) = {
      val sourceList = message.split("\\|")
      if (sourceList.length > 0) {
        sourceList(0) match {
          case kind if kind.equals(kindServer) => getServiceMessage
          case kind if kind.equals(kindClient) => getFromServiceMessage
          case _ => logError("MatchError: " + sourceList.mkString(","))
        }
      }

      def getServiceMessage = {
        logTime = sourceList(1)
        if (duration.equals("0") && !sourceList.isEmpty) duration = sourceList(2)
        serviceName = sourceList(3)
        if (fromService.isEmpty) fromService = if (isSelf) "itself" else sourceList(4)
        method = sourceList(5)
        path = sourceList(6)
      }

      def getFromServiceMessage = {
        if (logTime.isEmpty && !sourceList(1).isEmpty) sourceList(1)
        if (!sourceList(2).isEmpty) duration = sourceList(2)
        if (!sourceList(3).isEmpty) fromService = sourceList(3)
      }
    }

    for (i <- 0 until messageList.length) {
      getSpan(messageList(i))
    }
    val res = (logTime, duration, serviceName, fromService, method, path).toString()
    res.substring(1, res.length - 1) //为了去掉前后括号
  }
}