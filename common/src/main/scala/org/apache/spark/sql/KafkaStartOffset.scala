package org.apache.spark.sql

import com.aispeech.common.monitor.SendUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.OffsetSeqLog

/**
  * @author xiaomei.wang
  * @date 2019/11/1 17:54
  * @version 1.0
  */
case class KafkaStartOffset(spark: SparkSession, checkpoint: String,
                            kafkaOffset: String, kafkaTopic: String)
  extends Logging {

  def getStartOffsets: String = {
    // 1、charge checkpoint is exist or not
    val checkpointOffsetsPath = new Path(checkpoint, "offsets")
    val fs = checkpointOffsetsPath.getFileSystem(spark.sessionState.newHadoopConf())
    // 2、if exist， get start offset from checkpoint， if not，from config
    val offset = fs.exists(checkpointOffsetsPath) match {
      case true => getCheckpointOffsets(spark, checkpoint)
      case false => kafkaOffset
    }
    // 3、delete checkpoint
    fs.delete(new Path(checkpoint), true)
    // 4、send a message where the checkpoint is
    val r = new SendUtils()
    spark.sparkContext.getConf.get("spark.aispeech.monitor.urls")
      .split(",")
      .foreach(sendUrl => r.send(s"""[appName:${spark.sparkContext.appName}], start kafka offsets from:$offset""", sendUrl.trim))
    logWarning("----- start offsets " + offset)
    offset
  }

  private def getCheckpointOffsets(spark: SparkSession, checkpoint: String) = {
    def checkpointFile(name: String): String = new Path(new Path(checkpoint), name).toUri.toString

    val offsetLog = new OffsetSeqLog(spark, checkpointFile("offsets"))
    val (latestOffsetEpoch, _) = offsetLog.getLatest().getOrElse {
      throw new IllegalStateException(
        s"Offset log had no latest element. This shouldn't be possible because nextOffsets is" +
          s"an element.")
    }
    val latestOffsetPath = new Path(new Path(checkpointFile("offsets")), latestOffsetEpoch.toString).toUri.toString
    // current offset
    spark.sparkContext
      .textFile(latestOffsetPath, 1)
      .filter(_.contains(kafkaTopic))
      .collect()
      .mkString("")
  }
}
