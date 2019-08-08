package org.apache.spark

import org.apache.spark.aispeech.sink.KafkaSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/6/14 12:13
  * @version 1.0
  */
object ZipkinSpanJob extends Logging {

  val KIND_SERVER = "SERVER"
  val KIND_CLIENT = "CLIENT"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    logError("spark conf: " + spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")

    execute(spark)
  }

  def execute(spark: SparkSession): Unit = {
    val kafkaDF = readStream(spark)
    val spanDF = ZipkinLogUtils(spark, KIND_SERVER, KIND_CLIENT).getSpanDF(kafkaDF)
    val res = writeStream(spark, spanDF)
    res.awaitTermination()
  }

  def readStream(spark: SparkSession) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.brokers"))
      .option("subscribe", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.topics"))
      .option("startingOffsets", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.startOffsets"))
      .option("maxOffsetperTrigger", spark.sparkContext.getConf.get("spark.aispeech.read.kafka.maxOffsetperTrigger"))
      .option("includeTimestamp", true)
      .load()
  }

  def writeStream(spark: SparkSession, df: DataFrame) = {
    val conf = spark.sparkContext.getConf
    df.writeStream
      .option("checkpointLocation", conf.get("spark.aispeech.checkpoint"))
      .foreach(new KafkaSink(conf.get("spark.aispeech.read.kafka.brokers"), conf.get("spark.aispeech.write.kafka.topic")))
      .outputMode("update")
      .trigger(ProcessingTime(conf.get("spark.aispeech.trigger.time")))
      .start()
  }

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
