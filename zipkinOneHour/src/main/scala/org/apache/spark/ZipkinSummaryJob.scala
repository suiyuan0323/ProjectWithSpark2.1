package org.apache.spark

import java.text.SimpleDateFormat
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
    execute(spark)
  }

  def execute(spark: SparkSession) = {
    // 每次触发 当前小时的20分触发。
    val conf = spark.sparkContext.getConf
    val beginTimestamp = TimeUtils.getEndTimeStamp(-1)
    val endTimeStamp = beginTimestamp + 3600000
    val dayStr = new SimpleDateFormat("yyyy-MM-dd").format(beginTimestamp)
    val readSource = conf.get("spark.aispeech.read.es.index") + dayStr + "/" + conf.get("spark.aispeech.read.es.type")
    logError("_______start:" + beginTimestamp + ",end:" + endTimeStamp + ",indexDayStr:" + dayStr)
    val tempTableName = "zipkinTable"
    read(spark, readSource, "", beginTimestamp, endTimeStamp).createOrReplaceTempView(tempTableName)
    val writeSource = conf.get("spark.aispeech.write.es.index") + dayStr + "/" + conf.get("spark.aispeech.write.es.type")
    write(spark, writeSource, beginTimestamp, endTimeStamp, dayStr, tempTableName)
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
