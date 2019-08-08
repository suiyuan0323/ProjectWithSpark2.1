package org.apache.spark

import java.util.concurrent.TimeUnit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

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
    logError("-----conf" + conf.getAll.mkString(", "))

    val spark = SparkSession
      .builder()
      .appName("ZipkinSummaryJob" + conf.get("spark.aispeech.job.type"))
      .config("es.nodes.wan.only", "true")
      .config("es.nodes", conf.get("spark.aispeech.write.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.write.es.port"))
      .getOrCreate()

    logError("spark conf: " + spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")

    execute(spark)
  }

  /**
    * 主要逻辑
    *
    * @param spark
    */
  def execute(spark: SparkSession) = {
    val kafkaDF = readStream(spark)
    val summaryUtil = SpanSummary(spark)
    val spanDF = summaryUtil.parseSpan(kafkaDF)
    val aggDF = summaryUtil.doAgg(spanDF, spark.sparkContext.getConf.get("spark.aispeech.data.agg.window.duration"))
    writeStream(spark, aggDF).awaitTermination()
  }

  /**
    * 读数据
    *
    * @param spark
    * @return
    */
  def readStream(spark: SparkSession) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.brokers"))
      .option("subscribe", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.topics"))
      .option("startingOffsets", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.startOffsets"))
      .option("failOnDataLoss", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.failOnDataLoss"))
      .option("includeTimestamp", true)
      .load()
  }

  /**
    * 存入es
    *
    * @param spark
    * @param df
    */
  def writeStream(spark: SparkSession, df: DataFrame) = {
    val conf = spark.sparkContext.getConf
    val streamingWriter = df.writeStream
      .queryName("ES-Writer")
      .outputMode("append")
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .format("org.elasticsearch.spark.sql.sink.EsSinkProvider")
      .trigger(ProcessingTime.create(conf.getInt("spark.aispeech.trigger.time", 60), TimeUnit.SECONDS))
    streamingWriter.start()
  }

  def writeToConsole(spark: SparkSession, df: DataFrame) = {
    val conf = spark.sparkContext.getConf
    df.writeStream
      .outputMode("append")
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .trigger(ProcessingTime.create(conf.getInt("spark.aispeech.trigger.time", 60), TimeUnit.SECONDS))
      .format("console")
      .option("truncate", "false")
      .start()
  }
}
