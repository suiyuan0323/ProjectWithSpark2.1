package org.apache.spark

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONArray}
import org.utils.TimeUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

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
      .appName("ZipkinSummaryJob" + conf.get("spark.aispeech.job.type"))
      .config("es.nodes", conf.get("spark.aispeech.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.es.port"))
      .config("es.nodes.data.only", "true")
      .config("es.nodes.wan.only", "false")
      .config("es.index.auto.create", "true")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.read.unmapped.fields.ignore", "false")
      .getOrCreate()

    logError("spark conf: " + spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")


    initJob(spark)
  }

  /**
    * 如果配置了数据的时间，处理配置的数据，如果没配置，则处理当前时间点的前一个小时的数据
    *
    * @param spark
    */
  def initJob(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    val timeJson = conf.get("spark.aispeech.data.time")
    timeJson.isEmpty match {
      case true => {
        val beginTimestamp = TimeUtils.getEndTimeStamp(-1)
        execute(spark, beginTimestamp, beginTimestamp + 3600000, new SimpleDateFormat("yyyy-MM-dd").format(beginTimestamp))
      }
      case false => {
        val dayStrMap = JSON.parseObject(timeJson).getInnerMap
        val dayKeys = dayStrMap.keySet().iterator()
        while (dayKeys.hasNext) {
          val dayStr = dayKeys.next()
          val timeStampJsonArray: JSONArray = dayStrMap.get(dayStr).asInstanceOf[JSONArray]
          for (i <- 0 until timeStampJsonArray.size()) {
            val beginTimestamp = timeStampJsonArray.getJSONObject(i).getLong("beginTimestamp")
            val endTimestamp = timeStampJsonArray.getJSONObject(i).getLong("endTimestamp")
            execute(spark, beginTimestamp, endTimestamp, dayStr)
          }
        }
      }
    }
  }

  def execute(spark: SparkSession, beginTimestamp: Long, endTimestamp: Long, dayStr: String) = {
    val conf = spark.sparkContext.getConf
    logError("_______start:" + beginTimestamp + ",end:" + endTimestamp + ",indexDayStr:" + dayStr)
    val readSource = conf.get("spark.aispeech.read.es.index") + dayStr + "/" + conf.get("spark.aispeech.read.es.type")
    val tempTableName = "zipkinTable" + beginTimestamp
    read(spark, readSource, "", beginTimestamp, endTimestamp).createOrReplaceTempView(tempTableName)
    val writeSource = conf.get("spark.aispeech.write.es.index") + dayStr + "/" + conf.get("spark.aispeech.write.es.type")
    write(spark, writeSource, beginTimestamp, endTimestamp, dayStr, tempTableName)
  }

  /**
    * 从es读取数据，返回时间段内聚合数据
    *
    * @param spark
    * @param source index/type
    * @param querySql
    * @param beginTimestamp
    * @param endTimestamp
    */
  def read(spark: SparkSession, source: String, querySql: String, beginTimestamp: Long, endTimestamp: Long) = {
    import spark.implicits._
    spark.sqlContext.read
      .format("es")
      .load(source)
      .where(s""" beginTimestamp >= ${beginTimestamp} and endTimestamp <= ${endTimestamp} """)
      .filter($"fromService" =!= "all")
      .groupBy($"serviceName", $"fromService", $"path", $"method")
      .agg(sum($"query_amount").as("query_amount"),
        sum($"query_amount" * $"avg_duration").as("count_duration"),
        min($"min_duration").as("min_duration"),
        max($"max_duration").as("max_duration"))
      .select($"serviceName", $"fromService", $"path", $"method",
        $"query_amount", $"min_duration", $"max_duration",
        ($"count_duration" / $"query_amount").as("avg_duration").cast("int"))
  }

  /**
    *
    * @param spark
    */
  def write(spark: SparkSession, source: String, startTimestamp: Long, endTimestamp: Long, dayStr: String, tempTableName: String) = {
    import spark.implicits._
    spark.sql(
      s"""
         | select serviceName, fromService, method, path, avg_duration, max_duration, min_duration, query_amount,
         |  ${startTimestamp} as beginTimestamp,
         |  ${endTimestamp} as endTimestamp
         |  from ${tempTableName}
         """.stripMargin)
      .withColumn("beginTime", date_format(($"beginTimestamp" / 1000).cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00"))
      .withColumn("endTime", date_format(($"endTimestamp" / 1000).cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00"))
      .saveToEs(source)
  }
}
