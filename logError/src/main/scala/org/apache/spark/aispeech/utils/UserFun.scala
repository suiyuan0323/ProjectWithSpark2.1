package org.apache.spark.aispeech.utils

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateUtils
import org.spark_project.guava.collect.ArrayListMultimap

/**
  * @author xiaomei.wang
  * @date 2019/6/25 19:33
  * @version 1.0
  */
object UserFun {

  /**
    * 去重之后统计error\500\400
    *
    * @param messageList id, error
    * @param contactChar
    * @return error_count, 500_count, 400_count
    */
  def staticByWindow(messageList: List[String], contactChar: String) = {

    // 对id去重，返回去重后的 fun()
    val iterableAfterDistinct = UserFunCommon.distinctListByIndex(messageList, contactChar, 0,
      (itemList: List[String]) =>
        itemList match {
          case List(_, log_error, log_500, log_400) => (log_error.toInt, log_500.toInt, log_400.toInt)
        }
    )
    val res = iterableAfterDistinct.reduce((item1, item2) => (item1._1 + item2._1, item1._2 + item2._2, item1._3 + item2._3))
    res._1 + "," + res._2 + "," + res._3
  }
}



