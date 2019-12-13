package org.test

import java.text.SimpleDateFormat

/**
  * @author xiaomei.wang
  * @date 2019/8/2 17:41
  * @version 1.0
  */
object TimeTest {
  def main(args: Array[String]): Unit = {

    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
    val aaa = new SimpleDateFormat("yyyyMMdd")

    val time1 = "2019-12-12T9:00:00"
    val time2 = "2019-12-12T09:00:00"
    val time3 = "2019-12-12 9:00:00"
    val time4 = "2019-12-12 09:00:00"
    val time5 = "2019-12-12 12:0:0.406".replaceAll("T", " ")

    println(df.parse(time5).getTime)
  }

}
