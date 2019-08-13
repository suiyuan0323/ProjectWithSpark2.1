package org.apache.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author xiaomei.wang
  * @date 2019/8/8 17:34
  * @version 1.0
  */
case class ProcessData(spark: SparkSession) {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  def getData = {
    val jsonMap = JSON.parseObject(conf.get("spark.aispeech.read.hive.table.partitions")).getInnerMap
    val partitionKey = jsonMap.keySet().iterator()
    val filters = new StringBuffer()
    while (partitionKey.hasNext) {
      val filterKey = partitionKey.next()
      val filterValues = jsonMap.get(filterKey).asInstanceOf[String]
      filters.append("and  ")
      filters.append(s""" ${filterKey}.isin(${filterValues.split(",").toSeq: _*}) """)
    }
    val sourceDF = spark.sql(
      s"""
         | select ${conf.get("spark.aispeech.read.hive.table.columns")}
         |   from ${conf.get("spark.aispeech.read.hive.table.name")}
         """.stripMargin)
      .filter(filters.toString.substring(3))
    sourceDF.printSchema()
    sourceDF
  }

  /**
    * 交互次数要减去1
    *
    * @return
    */
  def execute = {
    val sourceDf = getData
    sourceDf.groupBy($"sessionId")
      .agg(min($"logTime").as("endtime"),
        max($"logTime").as("beginttime"),
        count($"recordId").as("interaction")
      )
  }
}
