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
    * 对id去重(如果id相同，取fromService字段不为空的record)，按照fromService分组，返回fromService和对应的升序排列的duration list
    *
    * @param messageList message格式  id|fromService|duration => Iterable(fromService,duration)
    * @return fromServiceA:durationList字符串:query_amount:avg_duration, fromServiceB:durationList字符串:query_amount:avg_duration
    */
  def collectFromSericeWithDuration(messageList: List[String], contactChar: String): String = {
    /* 对spanId去重，返回 Iterable((fromService, duration)) */
    def distinctId(messageList: List[String], contactChar: String) = {
      var res: scala.collection.mutable.Map[String, (String, String)] = scala.collection.mutable.Map()
      for (i <- 0 until messageList.length) {
        val message = messageList(i).split(contactChar)
        // 需要添加数据的两种情况：1、id不包含；2、id包含，但是当前fromService有name
        if (!res.contains(message(0)) // id已经存在
          || (res.contains(message(0)) && (!message(1).equals("itself") && !message(1).equals("unknown") && !message(1).contains(".")))) {
          res += (message(0) -> Tuple2(message(1), message(2)))
        }
      }
      res.values
    }

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
        logError("collectFromServiceWithDuration error, messageList: " + messageList.mkString(","))
        new Exception(e)
      }
    }
    builder.toString().substring(1)
  }

  /**
    * 对id去重，只取duration，返回所有去重后的数据：durationList、query_amount、avg_duration
    *
    * @param messageList
    * @param splitChar  原始数据之间的连接字符
    * @param returnChar 返回元素之间的连接字符
    * @return
    */
  def collectDuration(messageList: List[String], splitChar: String, returnChar: String) = {

    /* 对id去重之后，返回duration */
    def distinctId(messageList: List[String], contactChar: String) = {
      var res: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map()
      for (i <- 0 until messageList.length) {
        val message = messageList(i).split(contactChar)
        if (!res.contains(message(0))) res += (message(0) -> message(1).toLong)
      }
      res.values
    }

    val iterableAfterDistinct = distinctId(messageList, splitChar)
    s"%s${returnChar}%s${returnChar}%s".format(
      iterableAfterDistinct.toList.sortWith((a, b) => a.compareTo(b) < 0).mkString(splitChar),
      iterableAfterDistinct.size,
      iterableAfterDistinct.sum / iterableAfterDistinct.size)
  }

}
