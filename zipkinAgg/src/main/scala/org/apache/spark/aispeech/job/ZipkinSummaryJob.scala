package org.apache.spark.aispeech.job

import com.sparkDataAnalysis.common.monitor.SendUtils
import com.sparkDataAnalysis.common.listener.StructStreamingListener
import com.sparkDataAnalysis.pipeline.StreamQuery
import org.apache.spark.SparkConf
import org.apache.spark.aispeech.module.SpanSummary
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 对span做聚合
  * read: kafka
  * write: es
  *
  * @author xiaomei.wang
  * @date 2019/6/18 19:22
  * @version 1.0
  */
object ZipkinSummaryJob extends Logging {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName(conf.get("spark.aispeech.job.summaryType") + conf.get("spark.aispeech.job.type"))
      .config("es.nodes.wan.only", "true")
      .config("es.nodes", conf.get("spark.aispeech.write.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.write.es.port"))
      .getOrCreate()

    spark.sparkContext.setLogLevel(conf.get("spark.aispeech.job.log.level"))
    spark.streams.addListener(StructStreamingListener(spark))

    val monitorUrls = conf.get("spark.monitor.urls")
    var flag = true

    val query = conf.get("spark.aispeech.job.summaryType").trim match {
      case "summary" => StreamQuery.buildQuery(spark, SpanSummary(spark).doAgg)
      case "summaryAll" => StreamQuery.buildQuery(spark, SpanSummary(spark).doAggAll)
      case _ => throw new Exception("spark.aispeech.job.summaryType must is summary or summaryAll")
    }

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
}
