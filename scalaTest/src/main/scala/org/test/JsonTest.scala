package org.test

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * @author xiaomei.wang
  * @date 2019/8/1 18:37
  * @version 1.0
  */
object JsonTest {
  def main(args: Array[String]): Unit = {
    test1
  }

  /**
    * 需求：spark 的conf 中如何添加过滤条件，json形式
    * 目的：解析json，拼接到spark sql中
    */
  def test1 = {
    val testString = "{\"p_day\":\"20190725,20190726\",\"hour\":\"20\"}"
    val jsonMap = JSON.parseObject(testString).getInnerMap
    val partitionKey = jsonMap.keySet().iterator()
    while (partitionKey.hasNext) {
      val key = partitionKey.next()
      val values = jsonMap.get(key)
      println(values)
    }
    println(jsonMap)
  }

  /**
    * 需求：解析一端字符串，解析成json形式的字符串（kg_query_repeat)
    * 解决：用正则匹配解析，用fastjson组织json
    */
  def test2 = {
    val testString = "{recordId=de662e7965a5c9650a248337fc82f53d, question=啊, skillId=[S6538005968558190592]}"
    val pattern1 ="""(.*)\{(.*)=(.*)\,(.*)=(.*),(.*)=(.*)\}""".r
    val detail = new JSONObject()
    testString match {
      case pattern1(_, key1, value1, key2, value2, key3, value3) => {
        detail.put(key1.trim,value1.trim.replaceAll("\\[","").replaceAll("\\]",""))
        detail.put(key2.trim,value2.trim.replaceAll("\\[","").replaceAll("\\]",""))
        detail.put(key3.trim,value3.trim.replaceAll("\\[","").replaceAll("\\]",""))
      }
      case _ => println("not match")
    }
    println(detail.toJSONString)
  }
}
