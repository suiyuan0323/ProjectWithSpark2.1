package hive

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 统计kg_query，重复问题
  *
  * @author xiaomei.wang
  * @date 2019/7/25 18:00
  * @version 1.0
  */
object KgQuery {
  /* 保存表的分区 */
  val hivePDayField = "p_day"
  val hivePFlagField = "p_flag"
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val days = spark.sparkContext.getConf.get("spark.aispeech.data.days")

    days.isEmpty match {
      case true => {
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        val dayStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
        execute(spark, dayStr)
      }
      case false =>
        days.split(",").foreach(dayStr => execute(spark, dayStr))
    }
  }

  /**
    * 统计某天会话重复问题，结果保存到hive中
    *
    * @param spark
    * @param dayStr
    */
  def execute(spark: SparkSession, dayStr: String): Unit = {
    println("_____start " + dayStr)
    /* 临时表 */
    val tempTable: String = "tempTable"
    /* 拆开 */
    val skillFlag: Int = 1
    /* 未拆 */
    val skillsFlag: Int = 0
    createTable(spark)

    spark.udf.register("sortStringList", (arg: String) => arg.split(",").sorted.mkString(","))

    getSource(spark, dayStr, 0).createOrReplaceTempView(tempTable)

    saveToHive(spark, dayStr, skillsFlag,
      s"""
         | select sortStringList(skillIds) as skillId, question, count(1) as repeatNum
         | FROM ${tempTable}
         | group by sortStringList(skillIds), question
		""".stripMargin)

    saveToHive(
      spark, dayStr, skillFlag,
      s"""
         | select skillId, question, count(1) as repeatNum
         | from (SELECT explode(split(skillIds,',')) as skillId, question FROM ${tempTable}) a
         | group by a.skillId, a.question
			 """.stripMargin)

    println("_____end " + dayStr)
  }

  /**
    * 获取数据初步加工，只解析message中包含keyword的部分
    *
    * @param spark
    * @param dayStr
    * @return
    */
  def getSource(spark: SparkSession, dayStr: String, hourStr: Int): DataFrame = {
    import spark.implicits._
    val conf = spark.sparkContext.getConf

    val maxLength = conf.getInt("spark.aispeech.data.question.maxLength", 100)
    val replaceText = conf.get("spark.aispeech.data.question.overLength.replaceText")
    val getVerifiedStr = udf((context: String) => if (context.length > maxLength) replaceText else context)

    val toJson = udf((source: String) => {
      val pattern1 ="""(.*)\{(.*)=(.*)\,(.*)=(.*),(.*)=(.*)\}""".r
      val detail = new JSONObject()
      source match {
        case pattern1(_, key1, value1, key2, value2, key3, value3) => {
          detail.put(key1.trim, value1.trim.replaceAll("\\[", "").replaceAll("\\]", ""))
          detail.put(key2.trim, value2.trim.replaceAll("\\[", "").replaceAll("\\]", ""))
          detail.put(key3.trim, value3.trim.replaceAll("\\[", "").replaceAll("\\]", ""))
        }
        case _ => println("not match: " + source)
      }
      detail.toJSONString
    })

    spark.sql(s"select json from ${conf.get("spark.aispeech.read.table.name")} where day >= ${dayStr} and day <= ${Integer.parseInt(dayStr) + 1}")
      .select(get_json_object($"json", "$.module").as("module"),
        get_json_object($"json", "$.message").as("message"))
      .filter(substring(regexp_replace(get_json_object($"json", "$.logTime"), "-", ""), 0, 8) === dayStr
        and $"module" === conf.get("spark.aispeech.data.filter.module")
        and instr($"message", conf.get("spark.aispeech.data.filter.message.contain")) > 0)
      .select(toJson($"message").as("message"))
      .withColumn("questionText", get_json_object($"message", "$.question"))
      .filter("questionText is not null ")
      .select(regexp_replace(get_json_object($"message", "$.skillId"), " ", "").as("skillIds"),
        getVerifiedStr($"questionText").as("question"),
        get_json_object($"message", "$.recordId").as("recordId"))
  }

  def createTable(spark: SparkSession) = {
    val conf = spark.sparkContext.getConf
    spark.sql(
      s"""
         				 |create table if not exists ${conf.get("spark.aispeech.write.table.name")}(
         				 |    skillId string comment 'skillId',
         				 |    question string comment '问题',
         				 |    repeatNum int comment '重复次数'
         				 |)
         				 | partitioned by (${hivePDayField} string comment '日期', ${hivePFlagField} int comment '1拆0未拆')
         				 | STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY")
      """.stripMargin)
  }

  def saveToHive(spark: SparkSession, dayStr: String, flag: Int, querySql: String): Unit = {
    spark.sql(
      s"""
         | insert overwrite table ${spark.sparkContext.getConf.get("spark.aispeech.write.table.name")}
         | partition(${hivePDayField}=${dayStr}, ${hivePFlagField}=${flag})
         | ${querySql}
       """.stripMargin)
  }
}
