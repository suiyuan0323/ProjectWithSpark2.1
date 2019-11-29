package com.aispeech.realLine.streamingQuery.streamWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime, StreamingQuery}

/**
  * 一个df(sql), 可以以不同trigger用不同checkpoint以及不同queryname
  *
  * @author xiaomei.wang
  * @date 2019/10/23 10:21
  * @version 1.0
  */
case class ConsoleWriter(df: DataFrame,
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
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .trigger(ProcessingTime(triggerTime))
      .format("console")
      .option("truncate", "false")
  }
}
