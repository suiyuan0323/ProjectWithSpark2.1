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
    * @param messageList message格式  id|fromService|duration
    * @return Iterable((fromService, duration))
    */
  def distinctId(messageList: List[String], contactChar: String) = {
    var res: scala.collection.mutable.Map[String, (String, String)] = scala.collection.mutable.Map()
    for (i <- 0 until messageList.length) {
      val message = messageList(i).split(contactChar)
      // 需要添加数据的两种情况：1、id不包含；2、id包含，但是当前fromService有name
      if (!res.contains(message(0))
        || (res.contains(message(0)) && (!message(1).equals("itself") && !message(1).equals("unknown") && !message(1).contains(".")))) {
        res += (message(0) -> Tuple2(message(1), message(2)))
      }
    }
    res.values
  }

  /**
    * 按照fromService分组，返回fromService和对应的升序排列的duration list
    *
    * @param messageList message格式  id|fromService|duration => Iterable(fromService,duration)
    * @return fromServiceA:durationList字符串:query_amount:avg_duration, fromServiceB:durationList字符串:query_amount:avg_duration
    */
  def collectFromSericeWithDuration(messageList: List[String], contactChar: String): String = {
    val builder = StringBuilder.newBuilder
    try {
      val iterable = distinctId(messageList, contactChar)
      val res: ArrayListMultimap[String, String] = ArrayListMultimap.create()
      // 按照fromService分类
      iterable.iterator.foreach(item => res.put(item._1, item._2))

      val fromServiceSet = res.asMap().keySet().iterator()
      while (fromServiceSet.hasNext) {
        val fromService = fromServiceSet.next()
        val durationArray = res.asMap().get(fromService).toArray
        builder.append(",").append("%s:%s:%s:%s".format(fromService,
          durationArray.sortWith((a, b) => a.toString.toInt.compareTo(b.toString.toInt) < 0).mkString(contactChar),
          durationArray.size,
          durationArray.map { case (item: String) => item.toInt }.sum / durationArray.size
        ))
      }
    } catch {
      case e: Exception => {
        logError("collectFromSericeWithDuration error, messageList: " + messageList.mkString(","))
        new Exception(e)
      }
    }

    builder.toString().substring(1)
  }
}
