package org.test


import java.util
import scala.collection.JavaConversions._

/**
  * @author xiaomei.wang
  * @date 2019/10/24 17:42
  * @version 1.0
  */
object BitTest {
  def main(args: Array[String]): Unit = {

    val add = "1"
    val delete = "-1"
    val sss = "0"

    println(add.toInt.abs ^ 1)
    println(delete.toInt.abs ^ 1)
    println(sss.toInt.abs ^ 1)

    val treeset = new util.TreeSet[String]()
    treeset.add("sss")
    treeset.add("ddd")

    testtest(treeset.map(_.toString).toSeq: _*)

    val testMap = Map("a" -> "1", "b" -> "-1", "c" -> "0")

    val mapKeys = Set()

//    val values = mapKeys.toList.map { case keyStr => testMap.get(keyStr).get.toInt.abs }.sum
    val values = mapKeys.toList.map {
      testMap.get(_).get.toInt.abs
    }.sum

    println(values)
  }

  def testtest(stir: String*): Unit = {
    stir.foreach(println(_))

  }
}
