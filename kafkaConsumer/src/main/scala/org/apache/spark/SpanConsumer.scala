package org.apache.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/7/11 13:43
  * @version 1.0
  */
object SpanConsumer extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .appName("job" + "kafkaConsumer")
      .getOrCreate()

    logError("spark conf: " + conf.toDebugString)
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
    val resDf = SpanSummary(spark).parseSpan(kafkaDF)
    writeToConsole(spark, resDf).awaitTermination()
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
      .option("includeTimestamp", true)
      .load()
  }

  def writeToConsole(spark: SparkSession, df: DataFrame) = {
    val conf = spark.sparkContext.getConf
    df.writeStream
      .outputMode("append")
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .trigger(ProcessingTime.create(conf.getInt("spark.aispeech.trigger.seconds", 60), TimeUnit.SECONDS))
      .format("console")
      .option("truncate", "false")
      .start()
  }
}
