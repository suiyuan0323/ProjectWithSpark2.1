package com.sparkDataAnalysis.pipeline.output.streamWriter

import com.sparkDataAnalysis.pipeline.output.sink.KafkaSink
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @author xiaomei.wang
  * @date 2019/10/23 14:22
  * @version 1.0
  */
case class KafkaWriter(df: DataFrame,
                       conf: SparkConf,
                       queryName: String,
                       checkpoint: String,
                       triggerTime: String)
  extends StreamWriter {

  override def write(): StreamingQuery = {
    buildWriter().start()
  }

  override def buildWriter(): DataStreamWriter[Row] = {
    df.writeStream
      .option("checkpointLocation", checkpoint)
      .foreach(new KafkaSink(conf.get("spark.aispeech.write.kafka.brokers"), conf.get("spark.aispeech.write.kafka.topic")))
      .outputMode("update")
      .trigger(ProcessingTime(triggerTime))
    //.queryName(queryName)
    // .start()
  }
}
