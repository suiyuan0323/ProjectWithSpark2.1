package org.apache.spark.zipkin

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


/**
  * @author xiaomei.wang
  * @date 2019/9/1 14:06
  * @version 1.0
  */
case class ZipkinSpan(spark: SparkSession) {

  import spark.implicits._

  def processLog(source: DataFrame) = {

    val kindServer = "SERVER"

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

    source
      .select(explode(messageArray(get_json_object($"value", "$.message"))).as("message"))
      .select(get_json_object($"message", "$.id").alias("id"), // 都取出来，没有就是null
        get_json_object($"message", "$.traceId").alias("traceId"),
        get_json_object($"message", "$.timestamp").cast("float").alias("logTimeStr"),
        get_json_object($"message", "$.kind").alias("kind"),
        get_json_object($"message", "$.duration").alias("duration"),
        get_json_object($"message", "$.localEndpoint.serviceName").alias("serviceName"),
        get_json_object($"message", "$.remoteEndpoint.ipv4").alias("remoteIp"),
        get_json_object($"message", "$.tags").alias("tags"))
      .withColumn("logTime", from_unixtime($"logTimeStr" / 1000000).cast("timestamp"))
      .filter("""serviceName = 'qa'""")

  }
}
