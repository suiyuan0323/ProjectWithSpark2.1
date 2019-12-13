package com.sparkDataAnalysis.common.monitor

import org.apache.spark.sql.SparkSession

/**
  * spark job 发送告警
  *
  * @author xiaomei.wang
  * @date 2019/12/2 10:51
  * @version 1.0
  */
object SparkJobMonitor extends Serializable {
  def sendMessage(spark: SparkSession, message: String): Unit = {
    val r = new SendUtils()
    val conf = spark.sparkContext.getConf
    conf.get("spark.monitor.urls")
      .split(",")
      .foreach(sendUrl =>
        r.send(s"${spark.sparkContext.appName} is error. The message is ${message}",
          sendUrl.trim))
  }
}
