package org.aispeech.utils

import org.apache.spark.sql.functions.{get_json_object, regexp_replace, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/7/20 20:02
  * @version 1.0
  */
object HiveProcessUtils {

  /**
    * 取hive表中指定日志的数据
    *
    * @param spark
    * @param dayStr
    * @return
    */
  def getDataOfDay(spark: SparkSession, dayStr: String): DataFrame = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    spark.sql(
      s""" select json from ${conf.get("spark.read.table.name")}
         |  where ${conf.get("spark.read.table.partition.day")} >= ${dayStr}
         |  and ${conf.get("spark.read.table.partition.day")}<= ${Integer.parseInt(dayStr) + 1} """.stripMargin)
      .filter(substring(regexp_replace(get_json_object($"json", "$.logTime"), "-", ""), 0, 8) === dayStr)
  }
}
