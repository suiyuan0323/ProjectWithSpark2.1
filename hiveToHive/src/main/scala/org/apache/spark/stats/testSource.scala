package org.apache.spark.stats

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.JavaConversions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.hour


/**
  * @author xiaomei.wang
  * @date 2019/12/13 12:15
  * @version 1.0
  */
object testSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .config("spark.some.config.option", "some-value")
      .config("",1)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sourceString =
      """
        |{"code":200,"message":"OK",
        |"data":[{"type":1,"name":"ASR/TTS服务","rechargeUnitType":1,"defaultCtype":"webapi","biztype":"asr/tts","isValid":0,"ctypes":["webapi","mrcp"],"order":1},
        |{"type":6,"name":"一句话识别","rechargeUnitType":1,"defaultCtype":"webapi","biztype":"asr","isValid":1,"ctypes":["webapi"],"order":2},
        |{"type":7,"name":"语音合成","rechargeUnitType":1,"defaultCtype":"webapi","biztype":"tts","isValid":1,"ctypes":["webapi","mrcp"],"order":3},
        |{"type":2,"name":"录音文件转写","rechargeUnitType":2,"defaultCtype":"webapi","biztype":"asr","isValid":1,"ctypes":["webapi"],"order":4},
        |{"type":4,"name":"实时语音识别","rechargeUnitType":2,"defaultCtype":"websocket","biztype":"asr","isValid":1,"ctypes":["websocket","mrcp"],"order":5},
        |{"type":3,"name":"性别年龄识别","rechargeUnitType":1,"defaultCtype":"webapi","biztype":"asr","isValid":1,"ctypes":["webapi"],"order":6},
        |{"type":5,"name":"声纹验证","rechargeUnitType":1,"defaultCtype":"webapi","biztype":"asr","isValid":1,"ctypes":["webapi"],"order":7}]}
      """.stripMargin


    val sourceMap = JSON.parseObject(sourceString)
    val dataList: List[AnyRef] = sourceMap.getJSONArray("data").iterator().toList

    val dataMap = dataList.map {
      case item => {
        val itemJson = JSON.parseObject(item.toString)
        Record(itemJson.getString("type"), itemJson.getString("name"))
      }
    }

    val df = spark.createDataFrame(dataMap)
      .repartition(1)
        .coalesce()

    df.createOrReplaceTempView("nameTemplate")

//    spark.sql("select key as type, value as name from nameTemplate ").show()
    spark.sql(
      """
        | insert overwrite table dim_ba.prod_kf_producttype_info
        | select key as type, value as name from nameTemplate
      """.stripMargin)


    spark.stop()

    /* val df = spark.createDataFrame((1 to 59).map(i => Record(i, s"2019-12-12 12:1:$i")))
     df.createOrReplaceTempView("tem")
     spark.sql("select key, value, cast(value as timestamp) from tem")
       .show(false)
     spark.sql(" select value, cast(value as timestamp) as time from tem ")
       .withColumn("hour", hour($"time"))
       .show(false)*/
  }

  case class Record(key: String, value: String)

}
