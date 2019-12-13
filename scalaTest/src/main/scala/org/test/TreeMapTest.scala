package org.test

import com.alibaba.fastjson.{JSON, JSONObject}
import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap

/**
  * @author xiaomei.wang
  * @date 2019/8/29 19:08
  * @version 1.0
  */
object TreeMapTest {
  def main(args: Array[String]): Unit = {
    val source = Map("java" -> 10, "scala" -> 20, "js" -> 6, "sql" -> 8, "spark" -> 15, "hadoop" -> 2)
    val top3 = TreeMap(source.map { case (k, v) => (v, k) }.toArray: _*).takeRight(3).map { case (k, v) => (v, k) }
    println(JSON.toJSONString(top3.asJava, false))
  }

  def TreeMapToListNull()={

  }
}


