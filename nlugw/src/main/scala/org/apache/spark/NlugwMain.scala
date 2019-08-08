package org.apache.spark

import org.aispeech.utils.{HiveProcessUtils, TimeUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 功能：按天(日期格式'yyyyMMdd')统计nlu_gw模块，各种资源（eventName是jira列出来的）占总资源的比例
  * 说明：1、默认取当前时间的前一天为要处理的日期
  * 2、支持指定日期，设置main方法的参数，第一个参数为要处理的日期，可以传多个日期，逗号分隔，
  * 3、支持重运行，会overwrite对应的p_day分区数据
  * 数据：数据从hive表取，存到另一个hive表
  *
  * @author xiaomei.wang
  * @date 2019/7/19 15:23
  * @version 1.0
  */
object NlugwMain extends Logging {
  def main(args: Array[String]): Unit = {
    val dayStr = if (args.length > 0) args(0) else TimeUtils.initDay()
    val spark = initSpark()
    spark.sparkContext.setLogLevel("WARN")
    dayStr.split(",").foreach(execute(spark, _))
  }

  /**
    * 主逻辑：处理前一天的数据
    *
    * @param spark
    */
  def execute(spark: SparkSession, dayStr: String): Unit = {
    val tempTableName = "resultTempTable"
    createTableStatement(spark)
    readData(spark, dayStr).createOrReplaceTempView(tempTableName)
    writeData(spark, dayStr, tempTableName)
  }

  /**
    * 初始化sparkConf和sparkSession
    *
    * @return
    */
  def initSpark() = {
    val conf = new SparkConf()
    logError("_______conf: " + conf.toDebugString)

    SparkSession
      .builder()
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
    * 读取hive表前一天的数据，做清洗
    *
    * @param spark
    * @param dayStr
    * @param tempTable
    */
  def readData(spark: SparkSession, dayStr: String) = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    // 获取一天的原始json数据
    val dayData = HiveProcessUtils.getDataOfDay(spark, dayStr)
    // 过滤，取出所需要的字段
    val resultData = dayData
      .filter(get_json_object($"json", "$.module").isin(conf.get("spark.filter.modules").split(",").toSeq: _*)
        and get_json_object($"json", "$.eventName").isin(conf.get("spark.filter.eventNames").split(",").toSeq: _*))
      .select(get_json_object($"json", "$.eventName").as("eventName"))
      .groupBy($"eventName").count()
      .select($"eventName", $"count".as("eventName_count"))
    resultData.printSchema()
    resultData
  }

  /**
    * 写入hive表
    *
    * @param spark
    * @param dayStr
    * @param tempTable
    * @return
    */
  def writeData(spark: SparkSession, dayStr: String, tempTable: String) = {
    val conf = spark.sparkContext.getConf
    spark.sql(
      s""" insert overwrite table ${conf.get("spark.write.table.name")}
         | partition(${conf.get("spark.write.table.partition.day")}=${dayStr})
         | select eventName, eventName_count from ${tempTable}
      """.stripMargin)
  }

  /**
    * 创建hive表结构
    *
    * @param spark
    * @return
    */
  def createTableStatement(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    spark.sql(
      s""" create table if not exists ${conf.get("spark.write.table.name")}(
         |        eventName string comment '资源名称',
         |        eventName_count bigint comment '调用次数' )
         | partitioned by (${conf.get("spark.write.table.partition.day")} string comment '日期')
         | STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY")
      """.stripMargin)
  }
}
