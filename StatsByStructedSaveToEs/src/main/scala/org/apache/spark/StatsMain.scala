package org.apache.spark

import com.sparkDataAnalysis.common.monitor.SparkJobMonitor
import com.sparkDataAnalysis.pipeline.StreamQuery
import org.apache.spark.internal.Logging
import org.apache.spark.kfAPI.KfApiStatsProcess
import org.apache.spark.kfParallelism.KfParallelismProcess
import org.apache.spark.logError.ErrorLogProcess
import org.apache.spark.sql.SparkSession

/**
  * @author xiaomei.wang
  * @date 2019/12/2 18:42
  * @version 1.0
  */
object StatsMain extends Logging {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .master(conf.get("spark.job.master", "yarn"))
      .appName(conf.get("spark.aispeech.queryName", this.getClass.getName))
      .getOrCreate()

    conf.set("es.batch.write.retry.count", "30")
    conf.set("es.batch.write.retry.wait", "60")
    conf.set("es.http.timeout", "100s")
    conf.set("es.input.json", "true")

    spark.sparkContext.setLogLevel(spark.sparkContext.getConf.get("spark.job.log.level"))

    var flag = true
    while (flag) {
      try {
        logWarning("----- start query -------")
        flag = false
        // api
        StreamQuery.buildQuery(spark, KfApiStatsProcess(spark).statsByTimeWindow).start()

        // 通路
        // StreamQuery.buildQuery(spark, KfParallelismProcess(spark).statsByTimeWindow).start()

        // error
        //StreamQuery.buildQuery(spark, ErrorLogProcess(spark).statsByTimeWindow).start()

        spark.streams.awaitAnyTermination()
      } catch {
        case es: Exception => {
          SparkJobMonitor.sendMessage(spark, es.getMessage)
          flag = true
        }
        case error: Error => {
          SparkJobMonitor.sendMessage(spark, error.getMessage)
          flag = false
          throw error
        }
      }
    }
    spark.stop()
  }
}
