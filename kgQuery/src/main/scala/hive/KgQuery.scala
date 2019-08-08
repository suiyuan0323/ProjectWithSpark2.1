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
  /* 日期 */
  var days: String = _
  /* 原表 */
  var fromHiveTable: String = _
  /* 保存表 */
  val toHiveTable = "fact_ba.fact_kg_query_repeat"
  /* 保存表的分区 */
  val hivePDayField = "p_day"
  val hivePFlagField = "p_flag"
  /* 要处理的module */
  val module = "kg-query"
  /* 只分析message中含有关键字的log */
  val keyWord = "@t0:nvfLBE"
  /* 字段 */
  val fields = List("skillId", "question", "repeatNum")
  /* 拆开 */
  val skillFlag: Int = 1
  /* 未拆 */
  val skillsFlag: Int = 0

  /* 临时表 */
  val tempTable: String = "tempTable"
  val replaceQuestion = "提问语句超长"
  val maxLen: Int = 100

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("_____请输入数据源hive表名")
    } else {
      fromHiveTable = args(0)
      if (args.length > 1) {
        days = args(1)
      } else {
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        days = new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
      }
      val spark = SparkSession.builder()
        .enableHiveSupport()
        .appName(this.getClass.getName)
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

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
    spark.sql(getCreateTableStatement())

    val sortStringList: (String => String) = (arg: String) => arg.split(",").sorted.mkString(",")
    spark.udf.register("sortStringList", sortStringList)

    getSource(spark, dayStr, 0).createOrReplaceTempView(tempTable)

    val skillSql =
      s"""
         				 | select skillId, question, count(1) as repeatNum
         				 | from (SELECT explode(split(skillIds,',')) as skillId, question, recordId FROM ${tempTable}) a
         				 | group by a.skillId, a.question
			 """.stripMargin

    val skillsSql =
      s"""
         				 | select sortStringList(skillIds) as skillId, question, count(1) as repeatNum
         				 | FROM ${tempTable}
         				 | group by sortStringList(skillIds), question
		""".stripMargin

    saveToHive(spark, dayStr, skillsFlag, skillsSql)
    saveToHive(spark, dayStr, skillFlag, skillSql)
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
    spark.udf.register("getVerifiedStr", (context: String, len: Int, replaceStr: String) => if (context.length > len) replaceStr else context)
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
    val df1 = spark.sql(s"select json from ${fromHiveTable}  where day >= ${dayStr} and day<= ${Integer.parseInt(dayStr) + 1}")
      .filter(substring(regexp_replace(get_json_object($"json", "$.logTime"), "-", ""), 0, 8) === dayStr and
        get_json_object($"json", "$.module") === module and
        instr(get_json_object($"json", "$.message"), keyWord) > 0)
      .select(toJson(get_json_object($"json", "$.message")).as("message"))
      .withColumn("recordId", get_json_object($"message", "$.recordId"))
      .withColumn("skillIds", regexp_replace(get_json_object($"message", "$.skillId"), " ", ""))
      .withColumn("questionText", get_json_object($"message", "$.question"))
    //    df1.show()
    val df = df1.filter("questionText is not null ")
    df.createOrReplaceTempView("tempTable")
    spark.sql(s" select skillIds, getVerifiedStr(questionText, ${maxLen}, '${replaceQuestion}') as question, recordId from tempTable")
  }

  def getCreateTableStatement(): String = {
    val statement =
      s"""
         				 |create table if not exists ${toHiveTable}(
         				 |    skillId string comment 'skillId',
         				 |    question string comment '问题',
         				 |    repeatNum int comment '重复次数'
         				 |)
         				 | partitioned by (${hivePDayField} string comment '日期', ${hivePFlagField} int comment '1拆0未拆')
         				 | STORED AS PARQUET TBLPROPERTIES ("parquet.compression"="SNAPPY")
      """.stripMargin
    return statement
  }

  def saveToHive(spark: SparkSession, dayStr: String, flag: Int, querySql: String): Unit = {
    spark.sql(s"insert overwrite table ${toHiveTable} partition(${hivePDayField}=${dayStr}, ${hivePFlagField}=${flag}) ${querySql}")
  }
}
