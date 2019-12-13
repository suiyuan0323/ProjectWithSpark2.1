package org.apache.spark.aispeech

import com.sparkDataAnalysis.common.monitor.SendUtils
import com.sparkDataAnalysis.common.listener.StructStreamingListener
import com.sparkDataAnalysis.pipeline.StreamQuery
import org.apache.spark.SparkConf
import org.apache.spark.aispeech.process.{BaserverPvUv, PidPv, SourcePv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * @author xiaomei.wang
  * @date 2019/8/30 11:35
  * @version 1.0
  */
object BaserverSourceOrPidMain extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = getConf()
    val queryName = conf.get("spark.aispeech.queryName").trim.toLowerCase

    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[4]")
      .appName("business-" + queryName)
      .getOrCreate()

    logWarning("--- spark conf:" + conf.toDebugString)
    spark.sparkContext.setLogLevel(conf.get("spark.aispeech.job.log.level"))

    spark.streams.addListener(StructStreamingListener(spark))
    val monitorUrls = conf.get("spark.monitor.urls")

    val query = queryName match {
      case "pv_source_baserver" => StreamQuery.buildQuery(spark, SourcePv(spark).processData)
      case "pv_pid_baserver" => StreamQuery.buildQuery(spark, PidPv(spark).processData)
      case "pv_baserver" => StreamQuery.buildQuery(spark, BaserverPvUv(spark).processDataPv)
      case "uv_baserver" => StreamQuery.buildQuery(spark, BaserverPvUv(spark).processDataUv)
      case _ => throw new Exception(" please check queryName！")
    }
    var flag = true
    while (flag) {
      try {
        logWarning("----- start query -------")
        query.start()
        spark.streams.awaitAnyTermination()
      } catch {
        case es: Exception => {
          val r = new SendUtils()
          monitorUrls.split(",").foreach(sendUrl => r.send(es.getMessage, sendUrl.trim))
        }
        case error: Error => {
          val r = new SendUtils()
          monitorUrls.split(",").foreach(sendUrl => r.send(error.getMessage, sendUrl.trim))
          flag = false
          throw error
        }
      }
    }
    spark.stop()
  }

  def getConf() = {
    new SparkConf()
      .set("spark.aispeech.queryName", "pv_baserver")
      .set("spark.aispeech.job.source", "kafka")
      .set("spark.aispeech.job.sink", "console")
      .set("spark.aispeech.write.es.timeColumn", "beginTime")
      .set("spark.aispeech.job.log.level", "WARN")
      .set("spark.aispeech.data.module", "baserver")
      .set("spark.aispeech.data.eventName", "sys_input_output")
      .set("spark.aispeech.data.interval", "1 seconds")
      .set("spark.aispeech.data.watermark.delay", "1 seconds")
      .set("spark.aispeech.checkpoint", "/user/rsbj_ba_backend/business/")
      .set("spark.aispeech.trigger.time", "5 seconds")
      .set("spark.aispeech.read.kafka.startOffsets", "latest")
      .set("spark.aispeech.read.kafka.maxOffsetsPerTrigger", "6000000")
      .set("spark.aispeech.read.kafka.failOnDataLoss", "false")
      .set("spark.aispeech.read.kafka.brokers", "10.12.6.59:6667,10.12.6.60:6667,10.12.6.61:6667")
      .set("spark.aispeech.read.kafka.topics", "ba-prod-trace-log")
      .set("spark.aispeech.write.es.nodes", "10.24.1.44,10.24.1.23,10.24.1.24")
      .set("spark.aispeech.write.es.port", "9200")
      .set("spark.aispeech.write.es.type", "summary")
      .set("spark.aispeech.write.es.index", "business_stats-")
      .set("spark.monitor.urls", "https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2")
  }
}
