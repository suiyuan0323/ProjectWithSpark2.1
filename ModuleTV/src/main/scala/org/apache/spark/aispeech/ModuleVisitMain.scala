package org.apache.spark.aispeech

import com.sparkDataAnalysis.common.monitor.SendUtils
import com.sparkDataAnalysis.common.listener.StructStreamingListener
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.aispeech.process.VisitProcess
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

/**
  * @author xiaomei.wang
  * @date 2019/8/18 17:52
  * @version 1.0
  */
object ModuleVisitMain extends Logging {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .appName("business-module-pv-uv")
      .getOrCreate()

    logWarning("--- spark conf:" + conf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(StructStreamingListener(spark))

    val monitorUrls = conf.get("spark.monitor.urls")
    // 添加query
    val moduleJson = conf.get("spark.aispeech.data.query.jobs")
    val jsonArr = JSON.parseObject(moduleJson).getInnerMap
    val jobs = jsonArr.keySet().iterator()
    var queryArr = scala.collection.mutable.Map[String, DataStreamWriter[Row]]()
    while (jobs.hasNext) {
      val queryName = jobs.next()
      queryArr += (queryName -> VisitProcess.getQuery(spark, jsonArr.get(queryName).asInstanceOf[JSONObject], queryName))
    }
    var flag = true
    while (flag) {
      queryArr.foreach {
        case (queryName, query) =>
          try {
            query.start()
            logWarning(s"----- start query: ${queryName}---")
          } catch {
            case es: Exception => {
              val r = new SendUtils()
              monitorUrls.split(",").foreach(sendUrl => r.send(s"""【${queryName}】is error: ${es.getMessage}""", sendUrl.trim))
            }
            case error: Error => flag = false
          }
      }
      spark.streams.awaitAnyTermination()
    }
    spark.stop()
  }
}
