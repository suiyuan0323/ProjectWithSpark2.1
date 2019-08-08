package org.apache.spark.aispeech

import java.util.concurrent.TimeUnit
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * @author xiaomei.wang
  * @date 2019/6/30 12:19
  * @version 1.0
  */
object BaErrorLog extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName("BaErrorLog")
      .config("es.nodes.wan.only", "false")
      .config("es.nodes", conf.get("spark.aispeech.write.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.write.es.port"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val logStream = readStream(spark)
    val resStream = ErrorLogProcess(spark, logStream).satisticsByTime("", List(""))
    writeStream(spark, resStream).awaitTermination()
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
      .option("subscribe", conf.get("spark.aispeech.read.kafka.topcis"))
      .option("startingOffsets", conf.get("spark.aispeech.read.kafka.startOffsets"))
      .option("maxOffsetsPerTrigger", conf.get("spark.aispeech.read.kafka.maxOffsetsPerTrigger"))
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
    df.writeStream
      .queryName("ES-Writer")
      .outputMode("append")
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .option("es.resource.write", conf.get("spark.aispeech.write.es.index") + "*/" + conf.get("spark.aispeech.write.es.type"))
      .format("org.elasticsearch.spark.sql.sink.EsSinkProvider")
      .trigger(ProcessingTime.create(conf.getInt("spark.aispeech.trigger.seconds", 60), TimeUnit.SECONDS))
      .start()
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
