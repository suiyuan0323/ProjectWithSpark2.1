package org.apache.spark.common

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/8/15 14:18
  * @version 1.0
  */
object Utils {

  def initSpark(jobName: String) = {
    SparkSession.builder()
      .appName(jobName)
      .getOrCreate()
  }

  /**
    * 从kafka中读数据
    *
    * @param spark
    * @return
    */
  def readStream(spark: SparkSession,
                 kafkaServers: String,
                 subscribe: String,
                 startingOffsets: String,
                 failOnDataLoss: String,
                 maxOffsetsPerTrigger: String) = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", subscribe)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
  }

  /**
    * console展示
    *
    * @param spark
    * @param df
    * @return
    */
  def writeToConsole(df: DataFrame, checkpoint: String, triggerTime: String, name: String) = {
    df.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint + name)
      .trigger(ProcessingTime.create(triggerTime.toInt, TimeUnit.SECONDS))
      .format("console")
      .queryName(name)
      .option("truncate", "false")
      .start()
  }


  def writeToEs(df: DataFrame, checkpoint: String, triggerTime: String, name: String,
                nodes: String, port: String, indexPrefix: String, esType: String) = {
    df.writeStream
      .queryName("ES-Writer")
      .outputMode("append")
      .option("es.nodes.wan.only", "false")
      .option("es.nodes", nodes)
      .option("es.port", port)
      .option("checkpointLocation", checkpoint + name)
      .format("org.elasticsearch.spark.sql.sink.EsSinkProvider")
      .trigger(ProcessingTime.create(triggerTime.toInt, TimeUnit.SECONDS))
      .queryName(name)
      .start()
  }
}
