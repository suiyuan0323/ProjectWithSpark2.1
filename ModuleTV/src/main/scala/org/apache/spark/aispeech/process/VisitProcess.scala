package org.apache.spark.aispeech.process

import com.sparkDataAnalysis.pipeline.input.streamReader.KafkaStreamReader
import com.sparkDataAnalysis.pipeline.output.streamWriter.EsWriter
import com.alibaba.fastjson.JSONObject
import com.sparkDataAnalysis.pipeline.StreamQuery
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, KafkaStartOffset, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/8/7 11:00
  * @version 1.0
  */
object VisitProcess {

  /**
    * 获得module的访问量
    *
    * @param spark
    * @param queryConf ：query sql 的配置
    * @param kafkaDF
    * @param queryName
    * @return
    */
  def getQuery(spark: SparkSession, queryConf: JSONObject, queryName: String) = {

    val conf = spark.sparkContext.getConf
    // read from kafka
    val readSource = jobSource(spark, queryName)
    // 初始化job的配置, 写入sink
    jobSink(conf, sqlProcess(spark, readSource, queryName, queryConf), queryName)
  }

  def sqlProcess(spark: SparkSession, kafkaDF: DataFrame, queryName: String, queryConf: JSONObject) = {
    import spark.implicits._
    val (kind, selectMap, filterMap, groupColumns, distinctColumns) = parseJobConf(queryConf)
    buildSQL(spark, kind, kafkaDF, selectMap, filterMap, groupColumns, distinctColumns)
      .select($"module", $"eventName",
        date_format($"window.start".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("beginTime"), // time
        date_format($"window.end".cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").as("endTime"), // time
        $"window.start".cast("long").*(1000).as("beginTimestamp"), // long
        $"window.end".cast("long").*(1000).as("endTimestamp"), // long
        $"count")
      .createOrReplaceTempView(queryName + "TempTable")
    val res = spark.sql(
      s""" select "${queryName}" as stats_code,
         |        "${queryName}" as statsItem,
         |        count as resultNum,
         |        '' as resultDetails,
         |        beginTime, endTime, beginTimestamp, endTimestamp
         | from ${queryName}TempTable
       """.stripMargin)
    res.printSchema()
    res
  }

  def jobSource(spark: SparkSession, queryName: String) = {
    val conf = spark.sparkContext.getConf
    conf.get("spark.aispeech.job.source").trim.toLowerCase match {
      case "kafka" => KafkaStreamReader(
        spark,
        getConfValue(conf, "spark.aispeech.read.kafka.brokers", queryName),
        getConfValue(conf, "spark.aispeech.read.kafka.topics", queryName),
        getConfValue(conf, "spark.aispeech.read.kafka.maxOffsetsPerTrigger", queryName),
        KafkaStartOffset(spark,
          conf.get("spark.aispeech.checkpoint") + queryName,
          getConfValue(conf, "spark.aispeech.read.kafka.startOffsets", queryName),
          conf.get("spark.aispeech.read.kafka.topics")).getStartOffsets,
        getConfValue(conf, "spark.aispeech.read.kafka.failOnDataLoss", queryName)
      ).buildReader()
        .load()
      case _ => throw new Error("----- error source")
    }
  }

  def jobSink(conf: SparkConf, resultDF: DataFrame, queryName: String) = {
    conf.get("spark.aispeech.job.sink").trim.toLowerCase match {
      case "es" => EsWriter(
        resultDF,
        conf,
        queryName,
        conf.get("spark.aispeech.checkpoint") + queryName,
        conf.get("spark.aispeech.trigger.time"))
        .buildWriter()
      case _ => throw new Error("----- error sink")
    }
  }

  /**
    * build sql
    *
    * @param spark
    * @param kind
    * @param kafkaDF
    * @param selectMap
    * @param filterMap
    * @param groupColumns
    * @param distinctColumns
    * @return
    */
  private def buildSQL(spark: SparkSession,
                       kind: String,
                       kafkaDF: DataFrame,
                       selectMap: Map[String, String],
                       filterMap: Map[String, String],
                       groupColumns: Option[Array[String]],
                       distinctColumns: Option[Array[String]]) = {

    import spark.implicits._

    val conf = spark.sparkContext.getConf
    var res = kafkaDF.selectExpr("cast(value as String)")

    def getColumnFromJson(columnExpr: String): Column = {
      val exprs = columnExpr.split(",")
      var col: Column = trim(get_json_object($"value", exprs(0)))
      for (i <- 1 until exprs.length) {
        col = trim(get_json_object(col, exprs(i)))
      }
      col
    }

    selectMap.foreach {
      case ("logTime", columnExpr) =>
        res = res.withColumn("logTime", getColumnFromJson(columnExpr).cast("timestamp")).filter($"logTime".isNotNull)
          .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      case (columnName, columnExpr) => {
        res = res.withColumn(columnName, getColumnFromJson(columnExpr)).filter($"${
          columnName
        }".isNotNull)
      }
      case _ =>
    }

    filterMap.foreach {
      case (columnName: String, columnValues: String) =>
        res = res.filter($"${
          columnName
        }".isin(columnValues.split(",").toSeq: _*))
      case _ =>
    }

    /* distinctColumns.isEmpty match {
       case true =>
       case _ => res = res.dropDuplicates(distinctColumns.get)
     }*/
    val timeInterval = conf.get("spark.aispeech.data.interval")

    distinctColumns.isEmpty match {
      case true =>
        res = res
          .groupBy(window($"logTime", timeInterval, timeInterval), $"module", $"eventName")
          .count()
      case _ =>
        res = res
          .groupBy(window($"logTime", timeInterval, timeInterval), $"module", $"eventName")
          .agg(approx_count_distinct("sessionId").alias("count"))
    }

    res.printSchema()
    res
  }

  /**
    * queryConf => select、filter、group、distinct
    *
    * @param queryConf
    * @return
    */
  def parseJobConf(queryConf: JSONObject) = {

    import scala.collection.JavaConversions._

    (queryConf.get("kind").asInstanceOf[String],
      queryConf.get("select").asInstanceOf[JSONObject].toMap.asInstanceOf[Map[String, String]],
      queryConf.get("filter").asInstanceOf[JSONObject].toMap.asInstanceOf[Map[String, String]],
      Option(queryConf.get("group").asInstanceOf[String].isEmpty match {
        case true => null
        case _ => queryConf.get("group").asInstanceOf[String].split(",")
      }),
      Option(queryConf.get("distinct").asInstanceOf[String].isEmpty match {
        case true => null
        case _ => queryConf.get("distinct").asInstanceOf[String].split(",")
      })
    )
  }

  /**
    * 如果没有配置confKey + "." + jobName，则取confKey的值
    *
    * @param conf
    * @param confKey
    * @param name
    * @return
    */
  def getConfValue(conf: SparkConf, confKey: String, name: String): String = {
    conf.contains(confKey + "." + name) match {
      case true => conf.get(confKey + "." + name)
      case false => conf.get(confKey)
    }
  }
}
