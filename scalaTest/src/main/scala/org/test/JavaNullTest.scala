package org.test

import org.JavaNull

/**
  * @author xiaomei.wang
  * @date 2019/11/20 15:55
  * @version 1.0
  */
object JavaNullTest {

  def main(args: Array[String]): Unit = {
    val javaNull = new JavaNull()
    val res = Option(javaNull.nullTest()).getOrElse("")
    if (res.isEmpty) {
      println("---- null")
    }
  }

}
