package com.aispeech.realLine.streamingQuery.streamReader

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/10/23 10:56
  * @version 1.0
  */
case class KafkaStreamReader(spark: SparkSession,
                             kafkaServers: String,
                             subscribe: String,
                             maxOffsetsPerTrigger: String,
                             startingOffsets: String,
                             failOnDataLoss: String = "true") {

  def buildReader() = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", subscribe)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", failOnDataLoss)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
  }
}
