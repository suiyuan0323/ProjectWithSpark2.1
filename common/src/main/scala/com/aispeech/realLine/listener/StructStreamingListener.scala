package com.aispeech.realLine.listener

import com.aispeech.common.monitor.SendUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

/** 异步api
  *
  * @author xiaomei.wang
  * @date 2019/8/15 15:44
  * @version 1.0
  */
case class StructStreamingListener(spark: SparkSession) extends StreamingQueryListener{

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
//    logWarning("----- Query started at " + Calendar.getInstance().getTime
//      + "! [ name: " + event.name
//      + ", id: " + event.id
//      + ", runId: " + event.runId + "]")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    event.progress.processedRowsPerSecond
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    event.exception.get match {
      case message: String => {
        val sendMessage = "[ appName：" + spark.sparkContext.appName + "] is stop, exception is :" + message
        val r = new SendUtils()
        spark.sparkContext.getConf.get("spark.aispeech.monitor.urls").split(",").foreach(sendUrl => r.send(sendMessage, sendUrl.trim))
      }
      case _ =>
    }
  }
}
