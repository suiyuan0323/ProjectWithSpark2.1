package com.aispeech.realLine.streamingQuery

import com.aispeech.realLine.streamingQuery.streamReader.KafkaStreamReader
import com.aispeech.realLine.streamingQuery.streamWriter.{ConsoleWriter, EsWriter, KafkaWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, KafkaStartOffset, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/10/23 14:14
  * @version 1.0
  */
object StreamQuery {

  // 所有的都是 ，checkpoint是配置的checkpoint+queryName
  def buildQuery(spark: SparkSession, sqlProcess: (DataFrame) => DataFrame) = {
    val conf = spark.sparkContext.getConf
    // source
    val readSource = jobSource(spark)
    // sink
    jobSink(conf, sqlProcess(readSource))
  }

  def jobSource(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    conf.get("spark.aispeech.job.source").trim.toLowerCase match {
      case "kafka" => KafkaStreamReader(
        spark,
        conf.get("spark.aispeech.read.kafka.brokers"),
        conf.get("spark.aispeech.read.kafka.topics"),
        conf.get("spark.aispeech.read.kafka.maxOffsetsPerTrigger"),
        KafkaStartOffset(spark,
          conf.get("spark.aispeech.checkpoint") + conf.get("spark.aispeech.queryName"),
          conf.get("spark.aispeech.read.kafka.startOffsets"),
          conf.get("spark.aispeech.read.kafka.topics")).getStartOffsets,
        conf.get("spark.aispeech.read.kafka.failOnDataLoss")
      ).buildReader()
        .load()
      case _ => throw new Error("----- error source")
    }
  }

  def jobSink(conf: SparkConf, df: DataFrame) = {

    var resDF = conf.contains("spark.aispeech.write.repartition") match {
      case true => df.repartition(conf.getInt("spark.aispeech.write.repartition", 3))
      case false => df
    }

    resDF = conf.contains("spark.aispeech.write.coalesce") match {
      case true => df.repartition(conf.getInt("spark.aispeech.write.coalesce", 3))
      case false => df
    }

    conf.get("spark.aispeech.job.sink").trim.toLowerCase match {
      case "console" => ConsoleWriter(
        resDF,
        conf,
        conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.checkpoint") + conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.trigger.time")
      ).buildWriter()
      case "kafka" => KafkaWriter(
        resDF,
        conf,
        conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.checkpoint") + conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.trigger.time")
      ).buildWriter()
      case "es" => EsWriter(
        resDF,
        conf,
        conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.checkpoint") + conf.get("spark.aispeech.queryName"),
        conf.get("spark.aispeech.trigger.time")
      ).buildWriter()
      case _ => throw new Error("----- error sink")
    }
  }
}
