package org.apache.spark

import org.apache.spark.common.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.zipkin.{ZipkinQA, ZipkinSpan}

/**
  * @author xiaomei.wang
  * @date 2019/9/1 13:25
  * @version 1.0
  */
object DemoJob extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = Utils.initSpark(this.getClass.getName)
    logWarning("---- spark conf : " + spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")
    execute(spark)
    spark.streams.awaitAnyTermination()
  }

  def execute(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf

    val kafkaStream = Utils.readStream(
      spark,
      kafkaServers = conf.get("spark.aispeech.read.kafka.brokers"),
      subscribe = conf.get("spark.aispeech.read.kafka.topics"),
      startingOffsets = conf.get("spark.aispeech.read.kafka.startOffsets"),
      failOnDataLoss = conf.get("spark.aispeech.read.kafka.failOnDataLoss"),
      maxOffsetsPerTrigger = conf.get("spark.aispeech.read.kafka.maxOffsetsPerTrigger")
    ).selectExpr("cast(value as String)")

    //    val res = BaserverTopN(spark).processLog(kafkaStream)
    //    val res = ZipkinQA(spark).processLog(kafkaStream)
    val res = ZipkinSpan(spark).processLog(kafkaStream)

    Utils.writeToConsole(
      res,
      conf.get("spark.aispeech.checkpoint"),
      conf.get("spark.aispeech.trigger.time"),
      "test")
  }
}
