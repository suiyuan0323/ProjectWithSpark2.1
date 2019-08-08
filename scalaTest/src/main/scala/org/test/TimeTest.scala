package org.test

import java.text.SimpleDateFormat

/**
  * @author xiaomei.wang
  * @date 2019/8/2 17:41
  * @version 1.0
  */
object TimeTest {
  def main(args: Array[String]): Unit = {
    val testDay = "20190730"
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val aaa = new SimpleDateFormat("yyyyMMdd")
    df.format(aaa.parse(testDay))
  }

}
