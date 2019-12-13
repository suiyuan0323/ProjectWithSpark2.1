package org.apache.spark.aispeech.job

import java.text.SimpleDateFormat

import com.sparkDataAnalysis.common.timeAispeechUtils.TimeUtils
import org.apache.spark.SparkConf
import org.apache.spark.aispeech.module.SummaryProcessOffline
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * @author xiaomei.wang
  * @date 2019/11/25 23:31
  * @version 1.0
  */
object SummaryOfflineJob extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName(conf.get("spark.aispeech.job.name"))
      .config(conf)
      .config("es.nodes", conf.get("spark.aispeech.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.es.port"))
      .config("es.nodes.wan.only", conf.get("spark.aispeech.es.nodes.wan.only"))
      .config("es.scroll.keepalive", conf.get("spark.aispeech.read.es.scroll.keepalive"))
      .config("es.scroll.size", conf.get("spark.aispeech.read.es.scroll.size"))
      .config("es.batch.size.bytes", conf.get("spark.aispeech.write.es.batch.size.bytes"))
      .config("es.batch.size.entries", conf.get("spark.aispeech.write.es.batch.size.entries"))
      .config("es.batch.write.refresh", conf.get("spark.aispeech.write.es.batch.write.refresh"))
      .getOrCreate()

    // args是前一个小时
    logError("----args 0 ： date  " + args(0))

    spark.sparkContext.setLogLevel(conf.get("spark.aispeech.job.log.level"))

    try {
      conf.contains("spark.aispeech.data.timestamp") match {
        case true => execute(spark, conf.getLong("spark.aispeech.data.timestamp", 0L))
        case false => execute(
          spark,
          getIntPointTimestamp(
            args(0),
            conf.getInt("spark.aispeech.data.time.delay.minutes", 8),
            conf.get("spark.aispeech.write.es.timeType")
          )
        )
      }
    } catch {
      case ex: Exception => logError("--- error: " + ex.getMessage)
    } finally {
      spark.stop()
    }
  }

  /**
    * 返回整点时间戳，毫秒
    *
    * @param timeString
    * @param delayTimeMinute
    * @return
    */
  def getIntPointTimestamp(timeString: String, delayTimeMinute: Int, timeType: String): Long = {
    val timestampMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeString).getTime
    // 前移 delay的时间
    val standTimestampMs = timestampMs - delayTimeMinute * 60 * 1000
    // 需要的开始时间戳
    val timestamp = timeType match {
      case "5m" => standTimestampMs
      case "1h" => TimeUtils.getIntPointHourTimestamp(standTimestampMs)
      case "1d" => TimeUtils.getZeroPointDayTimestamp(standTimestampMs)
      case _ => throw new Exception("please check the timeType")
    }
    logWarning("_______time: current is " + timestampMs + ",thinking of delay the time is :" + standTimestampMs)
    timestamp
  }

  def execute(spark: SparkSession, beginTimestamp: Long) = {
    val conf = spark.sparkContext.getConf
    // 就用本地北京时间, 时间戳到毫秒
    val endTimeStamp = beginTimestamp + conf.getInt("spark.aispeech.data.time.interval.minutes", 5) * 60 * 1000
    // 必须是long，int不对
    val dayStr = new SimpleDateFormat("yyyy-MM-dd").format(beginTimestamp)

    val readSource =
      s"""${conf.get("spark.aispeech.read.es.index")}*/${conf.get("spark.aispeech.read.es.type")}"""

    val writeSource =
      s"""${conf.get("spark.aispeech.write.es.index")}${dayStr}/${conf.get("spark.aispeech.write.es.type")}"""

    logWarning("_______start job , start time :" + beginTimestamp +
      ",end:" + endTimeStamp +
      ",read source :" + readSource +
      ", write source :" + writeSource)

    val sourceDF = read(spark, readSource, beginTimestamp, endTimeStamp)
    val summaryDF = SummaryProcessOffline(spark)
      .getSummarySQL(sourceDF, (beginTimestamp / 1000).toInt, (endTimeStamp / 1000).toInt)
    write(spark, summaryDF, writeSource)

    logWarning("------ done")
  }

  def read(spark: SparkSession, readSource: String, beginTimestamp: Long, endTimeStamp: Long) = {
    val query =
      s"""
         |{
         |  "query":{
         |      "bool":{
         |          "must":[
         |                {"match_all":{}},
         |                {"range":{"logTime":{"gte":${beginTimestamp},"lte":${endTimeStamp},"format":"epoch_millis"}}}
         |           ]
         |      }
         |  }
         |}
       """.stripMargin
    logWarning("--- es query :" + query)
    spark.sqlContext.esDF(readSource, query)
    /*spark.sqlContext.read()
      .format("es")
      .load(readSource)
      .where($"logTime".cast("int").between(beginSecondTimestamp, endSecondTimeStamp))*/
  }

  def write(spark: SparkSession, df: DataFrame, writeSource: String) = {

    val conf = spark.sparkContext.getConf
    conf.get("spark.aispeech.job.source") match {
      case "es" =>
        df.saveToEs(writeSource)
      case "console" => df.show(10)
      case _ => {
        logWarning("---- error source")
        df.show(10)
      }
    }
  }

}
