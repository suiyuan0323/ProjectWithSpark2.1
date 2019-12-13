package org.apache.spark.stats.asr

import java.io.File

import com.sparkDataAnalysis.common.timeAispeechUtils.TimeUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 调用asr语言模型
  * result_src = comm:通用模型的识别结果
  * result_src = ab:二路模型的结果
  * result_src = c: 三路模型的结果
  * result_src = other，理论上这个不存在，如果不等于有comm、ab、c，就都改为other。
  */
object FactAsrParse extends Logging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val confDays = spark.sparkContext.getConf.get("spark.aispeech.data.days", "").trim
    confDays.isEmpty match {
      case true => {
        execute(spark, TimeUtils.reFormatTime(args(0), "yyyy-MM-dd HH:mm:ss", "yyyyMMdd"))
      }
      case false => confDays.split(",").foreach {
        case dayString: String => execute(spark, dayString)
      }
    }
  }

  def execute(spark: SparkSession, dayStr: String) = {
    readHive(spark, dayStr.toInt)
    writeHive(spark, dayStr)
  }

  def readHive(spark: SparkSession, dayStr: Int) = {
    import spark.implicits._
    import spark.sql
    val conf = spark.sparkContext.getConf
    val srcs = conf.get("spark.aispeech.data.srcs")
    val parseSrc = udf((srcStr: String) => if (srcs.split(",").contains(srcStr)) srcStr else "other")
    val parseSpkId = udf((spkIdStr: AnyRef) => parseABSpkId(spkIdStr))

    val wholeSql = (dayStr < 20191100)
    match {
      case true => {
        val queryColumn =
          """
            |  select
            |    product_id,
            |    get_json_object(json, "$.message.output.result.result_src") as result_src,
            |    get_json_object(json, "$.message.input.request.spkId") as spkId_all,
            |    from_unixtime(floor(get_json_object(json, "$.timestamp")), "yyyyMMdd") as dayStr
          """.stripMargin

        s"""
           | ${queryColumn}
           |   from ${conf.get("spark.aispeech.read.table")}
           |   where product_id is not null
           |   and p_day >= $dayStr
           |   and p_day <= ${dayStr + 1}
       """.stripMargin
      }
      case false => {
        val queryColumn =
          """
            | select
            |    product_id,
            |    result_src,
            |    spk_id spkId_all,
            |    from_unixtime(floor(time_stamp), 'yyyyMMdd') as dayStr
          """.stripMargin

        s"""
           | ${queryColumn}
           |  from ${conf.get("spark.aispeech.read.table.new")}
           | where product_id is not null
           |   and p_day >= $dayStr
           |   and p_day <= ${dayStr + 1}
        """.stripMargin
      }
    }

    logWarning("--- sql: " + wholeSql)
    val res = sql(wholeSql)
      .filter($"dayStr" === (dayStr) and $"result_src".isNotNull) // 日期, result_src存在
      .withColumn("src", parseSrc($"result_src"))
      .withColumn("spkId", when($"src".equalTo("ab"), parseSpkId($"spkId_all")) otherwise (""))
      .filter($"spkId" =!= "PASS") // 过滤ab不符合格式的
      .select($"dayStr", $"product_id", $"src", $"spkId")
      .coalesce(conf.getInt("spark.sql.shuffle.partitions", 1))

    res.createOrReplaceTempView("tempTable")
  }

  def writeHive(spark: SparkSession, dayStr: String) = {
    val conf = spark.sparkContext.getConf
    val writeTable = conf.get("spark.aispeech.write.table")
    val writePartition = conf.get("spark.aispeech.write.partitionKey")

    conf.get("spark.job.sink") match {
      case "hive" => {
        spark.sql(getCreateTableStatement(writeTable, writePartition))
        spark.sql(
          s""" insert overwrite table ${writeTable}
             | partition(${writePartition} = ${dayStr})
             | select product_id, src, spkId from tempTable
             | """.stripMargin)
      }
      case "console" => {
        spark.sql("select product_id, src, spkId from tempTable").show()
      }
      case _ =>
    }
  }

  def getCreateTableStatement(tableName: String, partition: String): String = {
    val statement =
      s"""
         |create table if not exists  ${tableName} (
         |    product_id string comment '产品ID',
         |    src string comment '语言模型大类：comm、ab、c、other',
         |    spkId string comment '语言模型小类'
         |)
         | partitioned by (${partition} string comment '日期')
         | STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY")
      """.stripMargin
    return statement
  }

  /**
    * spkId, 清洗result_src = ab 的spkId
    * 满足： 1、以 ] 结尾，前面能找到对应的 [ , 2 、且[] 里面不是latest 。则spkId = [ 之前的
    * 否则： spkId用pass填充
    */
  def parseABSpkId(spkIdObject: AnyRef): String = {
    if (spkIdObject == null) "PASS"
    else {
      val spkId = spkIdObject.toString
      val startIndex = spkId.indexOf('[')
      val endIndex = spkId.indexOf(']')
      if (startIndex > 0 && endIndex > 0 && !spkId.substring(startIndex + 1, endIndex).equals("latest"))
        spkId.substring(0, spkId.indexOf('['))
      else "PASS"
    }
  }
}
