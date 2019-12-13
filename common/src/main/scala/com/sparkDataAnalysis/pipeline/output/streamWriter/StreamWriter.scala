package com.sparkDataAnalysis.pipeline.output.streamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}

/**
  * @author xiaomei.wang
  * @date 2019/10/23 10:25
  * @version 1.0
  */
trait StreamWriter {

  def write(): StreamingQuery

  def buildWriter(): DataStreamWriter[Row]

}
