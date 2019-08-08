package org.apache.spark.aispeech.utils

/**
  * @author xiaomei.wang
  * @date 2019/7/1 16:53
  * @version 1.0
  */
object UserFunCommon {

  /**
    * 对list中的message，按照contactChar拆分，指定的index作为key做distinct，对每一条message做func格式处理
    *
    * @param messageList
    * @param contactChar 拆分message
    * @param index       指出message中key的位置
    * @param func        对每一个distinct后的message，做格式化处理
    * @tparam T 返回数据的格式
    * @return 返回去重后数据
    */
  def distinctListByIndex[T](messageList: List[String], contactChar: String, index: Int, func: (List[String]) => T) = {
    var res: scala.collection.mutable.Map[String, T] = scala.collection.mutable.Map()
    for (i <- 0 until messageList.length) {
      val message = messageList(i).split(contactChar).toList
      if (!res.contains(message(index))) res += (message(index) -> func(message))
    }
    res.values
  }
}
