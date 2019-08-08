package org.apache.spark

import java.text.SimpleDateFormat
import java.util.Calendar
import com.alibaba.fastjson.JSON
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * 同步hive表到es
  * 功能1、同步前一天数据到es，hive.partitions字段需要设为空
  * 功能2、同步指定partitions数据到es
  *
  * @author xiaomei.wang
  * @date 2019/7/31 10:29
  * @version 1.0
  */
object SyncMain extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("es.nodes", conf.get("spark.aispeech.es.nodes"))
      .config("es.port", conf.get("spark.aispeech.es.port"))
      .config("es.nodes.data.only", "true")
      .config("es.nodes.wan.only", "false")
      .config("es.index.auto.create", "true")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.read.unmapped.fields.ignore", "false")
      .enableHiveSupport()
      .getOrCreate()

    logError("spark conf: " + spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.setLogLevel("WARN")
    execute(spark)
  }

  /**
    * 主逻辑
    *
    * @param spark
    */
  def execute(spark: SparkSession) = {
    spark.sparkContext.getConf.get("spark.aispeech.read.hive.table.names").split(",")
      .foreach(tableName => syncHiveTableToEs(spark, tableName))
  }

  /**
    * 同步单个table数据到es
    *
    * @param spark
    * @param tableName
    */
  def syncHiveTableToEs(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    var initSql = spark.sql(s" select * from ${tableName} ")
    if (JSON.parseObject(conf.get("spark.aispeech.read.hive.table.partitions")).isEmpty) {
      // 默认同步前一天的数据
      val cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      val day = new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
      val partitionField = conf.get("spark.aispeech.read.hive.table.default.partition.day.field")
      writeToEs(spark, initSql.filter($"${partitionField}" === day), day, partitionField)
    } else {
      // 同步指定日期数据
      val jsonMap = JSON.parseObject(conf.get("spark.aispeech.read.hive.table.partitions")).getInnerMap
      val partitionKey = jsonMap.keySet().iterator()
      var dayPartitionKey = ""
      // 添加除了day之外的partition
      while (partitionKey.hasNext) {
        val key = partitionKey.next()
        if (key.contains("day")) dayPartitionKey = key
        else initSql = initSql.filter($"${key}".isin(jsonMap.get(key).toString.split(",").toSeq: _*))
      }
      if (!dayPartitionKey.trim.isEmpty) {
        jsonMap.get(dayPartitionKey).toString.split(",")
          .foreach(day => writeToEs(spark, initSql.filter($"${dayPartitionKey}" === day), day, dayPartitionKey))
      }
    }
  }

  /**
    * 写入es，生成id
    *
    * @param sourceDF
    * @param source
    */
  def writeToEs(spark: SparkSession, sourceDF: DataFrame, day: String, partitionField: String) = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf
    val toFormat = "yyyy-MM-dd"
    val dayStr = formateDay(day, toFormat, "yyyyMMdd")
    val source = conf.get("spark.aispeech.write.es.index") + dayStr + "/" + conf.get("spark.aispeech.write.es.type")
    logError("______ save the hive source(day=" + dayStr + ") to es(index=" + source + ")")
    val columns = conf.get("spark.aispeech.read.hive.table.columns")
    val tableName = "tempTable"
    sourceDF.createOrReplaceTempView(tableName)
    val res = spark.sql(
      s""" select ${columns},
         |        date_format(to_date('${dayStr}'), "${toFormat}") as time
         |   from ${tableName} """.stripMargin)
    res.printSchema()
    res.saveToEs(source)
  }

  def formateDay(day: String, toFormat: String, fromFormat: String) = {
    val to = new SimpleDateFormat(toFormat)
    val from = new SimpleDateFormat(fromFormat)
    to.format(from.parse(day))
  }
}
