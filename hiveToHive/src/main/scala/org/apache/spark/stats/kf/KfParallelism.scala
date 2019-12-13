package org.apache.spark.stats.kf

import com.sparkDataAnalysis.common.monitor.SparkJobMonitor
import com.sparkDataAnalysis.common.timeAispeechUtils.TimeUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 每个
  */
object KfParallelism extends Logging {
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
      execute(spark, args(0))
    } catch {
      case ex: Exception =>
        SparkJobMonitor.sendMessage(spark, ex.getMessage)
        throw ex
    } finally {
      spark.stop()
    }
  }

  /**
    *
    * @param spark
    * @param time
    */
  def execute(spark: SparkSession, time: String) = {
    val p_day = TimeUtils.reFormatTime(time, "yyyy-MM-dd HH:mm:ss", "yyyyMMdd")
    val p_hour = TimeUtils.reFormatTime(time, "yyyy-MM-dd HH:mm:ss", "HH")
    logWarning(s"----- start job， day : ${p_day}, hour: ${p_hour}")
    val resultDF = statsAllHour(spark, p_day)
        .coalesce(spark.sparkContext.getConf.getInt("", 1))
    writeDayHour(spark, resultDF, p_day, p_hour)
    logWarning(s"----- end ")
  }

  def writeDayHour(spark: SparkSession, df: DataFrame, p_day: String, p_hour: String) = {
    import spark.implicits._
    logWarning(s"---- execute ${this.getClass.getName}.write")
    val conf = spark.sparkContext.getConf
    conf.get("spark.job.sink") match {
      case "console" => df.show(2000)
      case "hive" => {
        val tempTable = "spark_temp_table"
        df.filter($"day_hour".equalTo(s"${p_day} ${p_hour}"))
          .createOrReplaceTempView(tempTable)

        val sql1 =
          s"""
             | insert overwrite table ${conf.get("spark.write.table")}
             |   partition(p_day = ${p_day}, p_hour = ${p_hour}, type=1)
             | select ${conf.get("spark.write.columns")} from ${tempTable} where type=1
       """.stripMargin
        spark.sql(sql1)


        val sql2 =
          s"""
             | insert overwrite table ${conf.get("spark.write.table")}
             |   partition(p_day = ${p_day}, p_hour = ${p_hour}, type=2)
             | select ${conf.get("spark.write.columns")} from ${tempTable} where type=2
       """.stripMargin
        spark.sql(sql2)
      }
    }
  }

  /**
    * 求一天中所有hour的max： module、product_id、maxnum、day_hour、
    * p_day、p_hour、type
    *
    * @return
    */
  def statsAllHour(spark: SparkSession, p_day: String) = {
    import spark.implicits._
    logWarning(s"---- execute ${this.getClass.getName}.statsAllHour")
    val conf = spark.sparkContext.getConf
    // fs和mrcp模块，所有时刻的通路数
    val df = (
      // mrcp
      processDetail(spark, p_day,
        "connect_to_asr",
        "close_to_asr",
        conf.get("spark.read.table.uniMrcp"),
        "uniMrcp")
      )
      .union(
        // fs
        processDetail(spark, p_day,
          "charge",
          "call-end",
          conf.get("spark.read.table.freeswitch"),
          "freeswitch")
      )

    // 分module和分module+pid
    (
      df.groupBy($"module", $"hour", $"product_id")
        .agg(max($"online_num").as("maxnum"))
        .select(
          $"module",
          concat_ws(" ", lit(p_day), $"hour").as("day_hour"),
          $"maxnum",
          $"product_id",
          lit(1).as("type")
        )
      )
      .union(
        df.groupBy($"module", $"hour", $"log_time")
          .agg(sum($"online_num").as("num"))
          .groupBy($"module", $"hour")
          .agg(max($"num").as("maxnum"))
          .select(
            $"module",
            concat_ws(" ", lit(p_day), $"hour").as("day_hour"),
            $"maxnum",
            lit("").as("product_id"),
            lit(2).as("type"))
      )
  }

  /**
    * 当天截止到当前小时，所有时刻的通路数
    *
    * @param spark
    * @param p_day
    * @return
    */
  private def processDetail(spark: SparkSession, p_day: String,
                            callOn: String, callUp: String, hiveTable: String, module: String) = {
    import spark.implicits._
    val readSql =
      s"""
         |select
         |    cast(log_time as timestamp) as log_time,
         |    product_id,
         |    event_name,
         |    sum(
         |        case
         |            when event_name = '${callOn}' then 1
         |            else -1
         |        end
         |    ) over(
         |        partition by product_id
         |        order by  cast(log_time as timestamp) asc
         |    ) num
         |from
         |    ${hiveTable}
         |where
         |    p_day =${p_day}
         |    and session_id != ''
         |    and event_name in ('${callOn}', '${callUp}')
         |order by
         |    cast(log_time as timestamp)
       """.stripMargin
    logWarning("---- readSql : " + readSql)
    val sourceDF = spark.sql(readSql)
    val minSum = sourceDF.select(min($"num")).take(1)(0).getLong(0)
    // 修正数据

    sourceDF
      .select(
        lit(module).as("module"),
        hour($"log_time").as("hour"),
        $"log_time",
        $"product_id",
        $"num".+(0 - minSum).as("online_num")
      )
  }
}
