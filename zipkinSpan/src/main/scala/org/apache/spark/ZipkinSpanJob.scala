package org.apache.spark

import com.sparkDataAnalysis.common.monitor.SendUtils
import com.sparkDataAnalysis.common.listener.StructStreamingListener
import com.sparkDataAnalysis.pipeline.StreamQuery
import org.apache.spark.aispeech.{ZipkinLogSpanProcess, ZipkinLogUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/6/14 12:13
  * @version 1.0
  */
object ZipkinSpanJob extends Logging {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .master(conf.get("spark.aispeech.job.master", "yarn"))
      .appName(conf.get("spark.aispeech.queryName"))
      .getOrCreate()

    spark.sparkContext.setLogLevel(conf.get("spark.aispeech.job.log.level"))
    spark.streams.addListener(StructStreamingListener(spark))

    val monitorUrls = conf.get("spark.monitor.urls")
    //    val query = StreamQuery.buildQuery(spark, ZipkinLogUtils(spark, KIND_SERVER, KIND_CLIENT).getSpanDF)
    val query = StreamQuery.buildQuery(spark, ZipkinLogSpanProcess(spark).getSpanDF)
    var flag = true

    while (flag) {
      try {
        logWarning("----- start query -------")
        query.start()
        spark.streams.awaitAnyTermination()
      } catch {
        case es: Exception => {
          val r = new SendUtils()
          monitorUrls.split(",").foreach(sendUrl => r.send(es.getMessage, sendUrl.trim))
        }
        case error: Error => {
          val r = new SendUtils()
          monitorUrls.split(",").foreach(sendUrl => r.send(error.getMessage, sendUrl.trim))
          flag = false
          throw error
        }
      }
    }
    spark.stop()
  }

  def getConf() = {
    val conf = new SparkConf()

    conf
      .set("spark.es.mapping.id", "id")
      .set("spark.aispeech.job.master","local[10]")
      .set("spark.aispeech.job.source", "kafka")
      .set("spark.aispeech.job.sink", "console")
      .set("spark.aispeech.queryName", "zipkin_span_save")
      .set("spark.aispeech.job.log.level", "WARN")
      .set("spark.aispeech.checkpoint", "hdfs://insight/user/rsbj_ba_backend/business/")
      .set("spark.aispeech.read.kafka.startOffsets", "latest")
      .set("spark.aispeech.read.kafka.brokers", "10.12.6.57:6667,10.12.6.58:6667,10.12.6.59:6667")
      .set("spark.aispeech.read.kafka.topics", "ba-test-zipkin-log")
      .set("spark.aispeech.read.kafka.failOnDataLoss", "false")
      .set("spark.aispeech.read.kafka.maxOffsetsPerTrigger", "500000")
      .set("spark.aispeech.write.es.nodes.wan.only", "false")
      .set("spark.aispeech.write.es.nodes", "10.24.1.44,10.24.1.23,10.24.1.24")
      .set("spark.aispeech.write.es.port", "9200")
      .set("spark.aispeech.write.es.batch.size.bytes", "1mb")
      .set("spark.aispeech.write.es.batch.size.entries", "1000")
      .set("spark.aispeech.write.es.batch.write.refresh", "false")
      .set("spark.aispeech.write.es.batch.write.retry.wait", "30s")
      .set("spark.aispeech.write.es.batch.write.retry.count", "3")
      .set("spark.aispeech.write.es.type", "summary")
      .set("spark.aispeech.write.es.index", "zipkin_span-")
      .set("spark.aispeech.data.watermark.delay", "30 seconds")
      .set("spark.aispeech.span.window.duration", "6 minutes")
      .set("spark.aispeech.span.slide.duration", "60 seconds")
      .set("spark.aispeech.trigger.time", "60 seconds")
      .set("spark.monitor.urls", "https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2")
      .set("spark.memory.fraction", "0.80")
      .set("spark.memory.storageFraction", "0.30")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2048mb")
      .set("spark.yarn.driver.memoryOverhead", "2048mb")
      .set("spark.yarn.executor.memoryOverhead", "2048mb")
      .set("spark.cleaner.periodicGC.interval", "15min")
      .set("spark.sql.shuffle.partitions", "500")
      .set("spark.shuffle.io.maxRetries", "2")
      .set("spark.shuffle.io.retryWait", "2s")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.network.timeout", "400s")
      .set("spark.io.compression.snappy.blockSize", "8k")
      .set("spark.kryo.referenceTracking", "false")
      .set("spark.driver.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC")
      .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.scheduler.listenerbus.eventqueue.size", "100000")

    conf
  }
}
