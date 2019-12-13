package org.test

/**
  * @author xiaomei.wang
  * @date 2019/11/5 12:23
  * @version 1.0
  */
object SortedTest {
  def main(args: Array[String]): Unit = {
    val rowList = List(
      Array("a", "on", "4"),
      Array("b", "up", "9"),
      Array("a", "up", "10"),
      Array("a", "on", "6"),
      Array("t", "on"),
      Array("b", "on", "26"),
      Array("b", "up", "46"),
      Array("b", "on", "16"),
      Array("a", "up", "20"))

    val res = rowList
      .filter(_.size >= 3)
      .map {
        case columns => (columns(0), columns(1), columns(2).toInt)
      }
      .sortBy(_._3)
    println(res)
    // put rows into redis
  }

}
