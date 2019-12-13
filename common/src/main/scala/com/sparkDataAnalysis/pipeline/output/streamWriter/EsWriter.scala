package com.sparkDataAnalysis.pipeline.output.streamWriter

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @param df
  * @param queryName
  * @param checkpoint
  * @param triggerTime   : 30 seconds
  * @param esNodes
  * @param esPort
  * @param esIndexPrefix : es的index的template
  * @param esType        ： es的index的type
  * @param wanOnly
  * @author xiaomei.wang
  * @date 2019/10/23 10:16
  * @version 1.0
  */

case class EsWriter(df: DataFrame,
                    conf: SparkConf,
                    queryName: String,
                    checkpoint: String,
                    triggerTime: String) extends StreamWriter {

  override def write(): StreamingQuery = {
    buildWriter().start()
  }

  override def buildWriter(): DataStreamWriter[Row] = {
    val res = conf.contains("spark.aispeech.write.es.partition") match {
      case true => df.repartition(conf.getInt("spark.aispeech.write.es.partition", 3))
      case false => df
    }
    res.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .option("es.nodes.wan.only", conf.get("spark.aispeech.write.es.nodes.wan.only"))
      .option("es.nodes", conf.get("spark.aispeech.write.es.nodes"))
      .option("es.port", conf.get("spark.aispeech.write.es.port", "9200"))
      .option("es.batch.size.bytes", conf.get("spark.aispeech.write.es.batch.size.bytes", "2mb"))
      .option("es.batch.size.entries", conf.get("spark.aispeech.write.es.batch.size.entries", "5000"))
      .option("es.batch.write.refresh", conf.get("spark.aispeech.write.es.batch.write.refresh", "false"))
      .option("es.resource.write", conf.get("spark.aispeech.write.es.index") + "*/" + conf.get("spark.aispeech.write.es.type"))
      //      .option("es.input.json", "true")
      .format("org.elasticsearch.spark.sql.sink.EsSinkProvider")
      .trigger(ProcessingTime(triggerTime))
  }
}
