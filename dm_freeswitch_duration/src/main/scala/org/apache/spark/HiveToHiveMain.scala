package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 通用：hive to hive by day
  *
  * 1、create table if not exist
  * 2、process data
  * 3、overwrite to hive
  *
  * @author xiaomei.wang
  * @date 2019/8/8 16:19
  * @version 1.0
  */
object HiveToHiveMain extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = getSpark
    createTableIFNotExist(spark)
    val res = getResultData(spark)
    res.show(10)
  }

  def getSpark = {
    SparkSession.builder()
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
  }

  /**
    * step 1: create table
    *
    * @param spark
    * @return
    */
  def createTableIFNotExist(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    val statement =
      s"""
         |create table if not exists ${conf.get("spark.aispeech.write.table.name")}(
         |    ${conf.get("spark.aispeech.write.table.schema.columns")}
         |)
         | partitioned by (${conf.get("spark.aispeech.write.table.schema.partitions")})
         | STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY")
      """.stripMargin
    spark.sql(statement)
  }

  def getResultData(spark: SparkSession) = {
    ProcessData(spark).getData
   // ProcessData(spark).execute
  }

  def saveToHive() = {

  }
}
