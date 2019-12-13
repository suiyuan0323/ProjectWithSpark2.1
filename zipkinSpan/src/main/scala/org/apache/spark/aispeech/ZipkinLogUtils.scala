package org.apache.spark.aispeech

import com.alibaba.fastjson.JSON
import org.apache.spark.aispeech.udaf.MessageConcat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    // 1、清洗log， 过滤掉 serviceName、id、traceId、logTime、duration为空
    ZipkinLogCommon.getZipkinDFFromLogDF(spark, kindClient, kindServer, logDF, tempTableName)
    // 2、合成span
    val spanDF = spark.sql(
      s"""
         | select id, traceId, kind, duration, serviceName, logTime,
         |        if(kind = '${kindServer}', ifnull(remoteIp, 'unknown'), '') as remoteIp,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.method'), '') as method,
         |        if(kind = '${kindServer}', jsonTag(tags, 'http.path'), '') as path
         | from ${tempTableName}
          """.stripMargin)
      .filter(s"( kind = '${kindClient}' or ( kind = '${kindServer}' and method != '' and path != '' ))")
      .select(
        $"logTime", $"traceId", $"id",
        concat_ws("|", $"kind", $"logTime", $"duration", $"serviceName", $"remoteIp", $"method", $"path").as("message"))
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
        date_format($"span".getItem(0).cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("logTime")
      )
      .filter(""" serviceName is not null and serviceName != '' and logTime is not null """)
    spanDF
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
          case _ => logError(" --------- MatchError: " + sourceList.mkString(","))
        }
      }

      // 如果只有service：duration、fromService
      def getServiceMessage = {
        logTime = sourceList(1)
        serviceName = sourceList(3)
        method = sourceList(5)
        path = sourceList(6)
        if (duration.equals("0") && !sourceList.isEmpty) duration = sourceList(2)
        if (fromService.isEmpty) fromService = if (isSelf) "itself" else sourceList(4)
      }

      def getFromServiceMessage = {
        duration = sourceList(2)
        fromService = sourceList(3)
      }
    }

    for (i <- 0 until messageList.length) {
      try {
        getSpan(messageList(i))
      } catch {
        case ex: Exception => {
          logWarning("----- list: " + messageList(i))
          /*val r = new SendUtils()
          conf.get("spark.monitor.urls", "").split(",").foreach {
            case sendUrl =>
              r.send(s"----${spark.sparkContext.appName} error, function【combineMessageToSpan】，message is【${messageList(i)}】'",
                sendUrl.trim)
          }*/
        }
      }
    }
    val res = (logTime, duration, serviceName, fromService, method, path).toString()
    res.substring(1, res.length - 1) //为了去掉前后括号
  }
}
