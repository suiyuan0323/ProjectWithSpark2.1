package org.apache.spark

import com.sun.javafx.util.Logging
import org.apache.spark.module.Baserver
import org.apache.spark.sql.streaming.{ProcessingTime}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/8/5 18:18
  * @version 1.0
  */
object BaserverTvMain extends Logging {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    execute(spark)
  }

  def execute(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    val sourceDf = readStream(spark)
    val baserver = Baserver(spark,
      module = conf.get("spark.aispeech.data.filter.module"),
      eventName = conf.get("spark.aispeech.data.filter.eventName"),
      sourceDF = sourceDf)
    val moduleDf = baserver.initModuleSource()
    val countDF = baserver.statisticsByTime(moduleDf)

    spark.sparkContext
    val res = writeToConsole(spark, countDF)
    res.awaitTermination()
  }

  /**
    * 从kafka中读数据
    *
    * @param spark
    * @return
    */
  def readStream(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.get("spark.aispeech.read.kafka.brokers"))
      .option("subscribe", conf.get("spark.aispeech.read.kafka.topics"))
      .option("startingOffsets", conf.get("spark.aispeech.read.kafka.startOffsets"))
      .option("maxOffsetsPerTrigger", conf.get("spark.aispeech.read.kafka.maxOffsetsPerTrigger"))
      .load()
  }

  /**
    * console展示
    *
    * @param spark
    * @param df
    * @return
    */
  def writeToConsole(spark: SparkSession, df: DataFrame) = {
    val conf = spark.sparkContext.getConf
    df.writeStream
      .outputMode("append")
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .trigger(ProcessingTime(conf.get("spark.aispeech.trigger.time")))
      .format("console")
      .option("truncate", "false")
      .start()
  }
}
