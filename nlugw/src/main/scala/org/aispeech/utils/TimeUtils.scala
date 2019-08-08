package org.aispeech.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * @author xiaomei.wang
  * @date 2019/7/20 19:18
  * @version 1.0
  */
object TimeUtils {

  /**
    * 返回前一天得日期
    *
    * @return
    */
  def initDay(): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
  }
}
