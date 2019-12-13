package org.apache.spark.stats.common

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/12/5 17:24
  * @version 1.0
  */
object WriteUtils extends Logging{

  /**
    * 天级别
    *
    * @param spark
    * @param df
    * @param p_day
    * @return
    */
  def writeDay(spark: SparkSession, df: DataFrame, p_day: String) = {
    logWarning(s"---- execute ${this.getClass.getName}.write")
    val conf = spark.sparkContext.getConf
    conf.get("spark.job.sink") match {
      case "console" => df.show()
      case "hive" => {
        val tempTable = "spark_temp_table"
        df.createOrReplaceTempView(tempTable)
        spark.sql(
          s"""
             | insert overwrite table ${conf.get("spark.write.table")}
             |   partition(${conf.get("spark.write.day.partition")} = ${p_day})
             | select ${conf.get("spark.write.columns")} from ${tempTable}
       """.stripMargin)
      }
    }
  }

  def writeDayHour(spark: SparkSession, df: DataFrame, p_day: String, p_hour:String) = {
    logWarning(s"---- execute ${this.getClass.getName}.write")
    val conf = spark.sparkContext.getConf
    conf.get("spark.job.sink") match {
      case "console" => df.show()
      case "hive" => {
        val tempTable = "spark_temp_table"
        df.createOrReplaceTempView(tempTable)
        spark.sql(
          s"""
             | insert overwrite table ${conf.get("spark.write.table")}
             |   partition(p_day = ${p_day}, p_hour = ${p_hour})
             | select ${conf.get("spark.write.columns")} from ${tempTable}
       """.stripMargin)
      }
    }
  }
}
