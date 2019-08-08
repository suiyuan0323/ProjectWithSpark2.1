package org.apache.spark.aispeech.udf

import org.apache.spark.internal.Logging
import org.spark_project.guava.collect.ArrayListMultimap

/**
  * @author xiaomei.wang
  * @date 2019/6/23 17:51
  * @version 1.0
  */
object SparkUserUDF extends Logging {

  /**
    * 对spanId去重
    *
    * @param messageList message格式  id|duration
    * @return Iterable(duration)
    */
  def distinctId(messageList: List[String], contactChar: String) = {
    var res: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map()
    for (i <- 0 until messageList.length) {
      val message = messageList(i).split(contactChar)
      if (!res.contains(message(0))) res += (message(0) -> message(1).toLong)
    }
    res.values
  }


  /**
    * 对id去重，返回所有去重后durationList、query_amount、avg_duration
    *
    * @param messageList
    * @param splitChar  原始数据之间的连接字符
    * @param returnChar 返回元素之间的连接字符
    * @return
    */
  def collectDuration(messageList: List[String], splitChar: String, returnChar: String) = {
    // 对id去重之后，返回duration
    val iterableAfterDistinct = distinctId(messageList, splitChar)
    s"%s${returnChar}%s${returnChar}%s".format(
      iterableAfterDistinct.toList.sortWith((a, b) => a.compareTo(b) < 0).mkString(splitChar),
      iterableAfterDistinct.size,
      iterableAfterDistinct.sum / iterableAfterDistinct.size)
  }
}
