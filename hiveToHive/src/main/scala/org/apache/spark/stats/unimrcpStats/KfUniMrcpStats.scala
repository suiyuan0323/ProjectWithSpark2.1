package org.apache.spark.stats.unimrcpStats

import java.text.SimpleDateFormat
import java.util.Date

import com.sparkDataAnalysis.common.monitor.SparkJobMonitor
import com.sparkDataAnalysis.common.timeAispeechUtils.TimeUtils
import com.sparkDataAnalysis.common.udf.ConnectColumnUDF
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.stats.common.WriteUtils

/**
  * @author xiaomei.wang
  * @date 2019/12/5 16:50
  * @version 1.0
  */
object KfUniMrcpStats extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master(conf.get("spark.job.master", "yarn"))
      .appName(conf.get("spark.aispeech.queryName", this.getClass.getName))
      .getOrCreate()

    spark.sparkContext.setLogLevel(conf.get("spark.job.log.level"))

    try {
      stats(spark, args(0))
    } catch {
      case ex: Exception =>
        SparkJobMonitor.sendMessage(spark, ex.getMessage)
        throw ex
    } finally {
      spark.stop()
    }
  }

  def stats(spark: SparkSession, time: String) = {
    val p_day = TimeUtils.reFormatTime(time, "yyyy-MM-dd HH:mm:ss", "yyyyMMdd")
    logWarning(s"----- start query， day : ${p_day}")
    val sourceDF = read(spark, p_day)
    logWarning(s"----- start process ")
    val resultDF = process(spark, sourceDF)
    logWarning(s"----- start write ")
    WriteUtils.writeDay(spark, resultDF, p_day)
    logWarning(s"----- end ")
  }

  def process(spark: SparkSession, df: DataFrame) = {
    import spark.implicits._

    val connect_column = new ConnectColumnUDF("")
    val timestampMills = udf((date:String) => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(date.trim).getTime)
    val milltimeFormat = udf((date:Long) => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date(date)))

    df.filter($"session_id".isNotNull && $"session_id".notEqual(""))
      .withColumn("log_time_mill", timestampMills($"log_time"))
      .withColumn("charge_event_name", when($"event_name" === "charge", 1).otherwise(0))
      .withColumn("log_state", when($"event_name" === "close_to_asr", $"message__event").otherwise(""))
      .groupBy($"session_id")
      .agg(
        max($"product_id").as("product_id"),
        max($"phone_number").as("phone_number"),
        max($"log_time_mill").as("endtimestamp"),
        min($"log_time_mill").as("begintimestamp"),
        connect_column($"log_state").as("state"),
        sum($"charge_event_name").as("interaction")
      )
      .select(
        $"session_id",
        $"product_id",
        $"phone_number",
        milltimeFormat($"begintimestamp").as("begintime"),
        $"begintimestamp",
        milltimeFormat($"endtimestamp").as("endtime"),
        $"endtimestamp",
        $"endtimestamp".-($"begintimestamp").as("duration"),
        $"state",
        $"interaction"
      )
  }

  def read(spark: SparkSession, p_day: String, p_hour: String = "") = {
    val conf = spark.sparkContext.getConf

    val hourQuery = p_hour match {
      case "" => ""
      case hour: String =>
        s"and ${conf.get("spark.query.hour.partiiton")} = $hour "
    }

    val querySQL =
      s"""
         |select ${conf.get("spark.query.columns")}
         |  from ${conf.get("spark.query.table")}
         | where ${conf.get("spark.query.day.partition")} = '${p_day}'
         | $hourQuery
       """.stripMargin

    logWarning("---- query sql: " + querySQL)

    spark.sql(querySQL)
  }

}
