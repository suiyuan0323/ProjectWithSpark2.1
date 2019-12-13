package org.apache.spark.job.kf_api_hour

import java.text.SimpleDateFormat

import com.sparkDataAnalysis.common.monitor.SparkJobMonitor
import com.sparkDataAnalysis.common.timeAispeechUtils.TimeUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/12/10 18:24
  * @version 1.0
  */
object KfApiStatsHour extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName(conf.get("spark.job.query.name"))
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel(conf.get("spark.job.log.level"))

    try {
      args(0).isEmpty match {
        case true => throw new Exception("there is no beginTime, args is null")
        case false => {
          val timestampMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(args(0)).getTime
          val beginTimeMillis = TimeUtils.getIntPointHourTimestamp(timestampMs)
          // 毫秒
          execute(spark, beginTimeMillis, beginTimeMillis + conf.getInt("spark.data.interval.time", 0))
        }
      }
    }
    catch {
      case es: Exception => {
        SparkJobMonitor.sendMessage(spark, es.getMessage)
        throw new Exception(es.getMessage)
      }
    }
    finally {
      spark.stop()
    }
  }

  /**
    *
    * @param spark
    * @param beginTimestamp 毫秒
    * @param endTimestamp   毫秒
    */
  def execute(spark: SparkSession, beginTimestamp: Long, endTimestamp: Long) = {
    import spark.implicits._
    val sparkTempTable = "sparkAPITable"
    val day = TimeUtils.getTimeFromStamp(beginTimestamp, "yyyyMMdd")
    val hour = TimeUtils.getTimeFromStamp(beginTimestamp, "HH")

    // stats
    readES(spark, beginTimestamp, endTimestamp)
      .filter($"child_productId".isNotNull && !$"child_productId".equalTo(""))
      .select(
        $"module",
        $"productId".as("product_id"),
        $"child_productId".as("child_product_id"),
        $"request_count",
        $"external_request_count")
      .groupBy($"module", $"product_id", $"child_product_id")
      .agg(
        sum($"request_count").as("request_count"),
        sum($"external_request_count").as("external_request_count"))
      .select($"module",
        $"product_id",
        $"child_product_id",
        $"request_count",
        $"external_request_count")
      .createOrReplaceTempView(sparkTempTable)

    // save
    write(spark, sparkTempTable, day, hour)

  }

  def write(spark: SparkSession, sparkTempTable: String, day: String, hour: String) = {
    val conf = spark.sparkContext.getConf
    conf.get("spark.job.sink").trim.toLowerCase match {
      case "hive" => {
        spark.sql(
          s"""
             | insert overwrite table ${conf.get("spark.write.table")}
             |        partition(p_day ='${day}', p_hour='${hour}')
             | select module,
             |        product_id,
             |        child_product_id,
             |        request_count,
             |        external_request_count,
             |        '$day $hour' as day_hour
             |   from ${sparkTempTable}
       """.stripMargin)
      }
      case "console" => spark.sql(s"select * from ${sparkTempTable}").show()
      case _ => throw new Exception(" ------- the setting【spark.aispeech.job.sink】 is warning ")
    }
  }

  def readES(spark: SparkSession, beginTimestamp: Long, endTimestamp: Long) = {
    val conf = spark.sparkContext.getConf
    // 加 source  includes 不管用， 只取query
    val querySQL =
      s"""
         |{
         |  "query":{
         |      "bool":{
         |          "must":[
         |                {"match_all":{}},
         |                {
         |                  "range":{
         |                    "${conf.get("spark.data.es.time.column")}":{
         |                           "gte":${beginTimestamp},
         |                           "lte":${endTimestamp},
         |                           "format":"epoch_millis"
         |                       }
         |                   }
         |                }
         |           ]
         |      }
         |  }
         |}
       """.stripMargin
    logWarning("--- es query :" + querySQL)
    spark.esDF(conf.get("spark.es.resource.read"), querySQL)
  }

}
