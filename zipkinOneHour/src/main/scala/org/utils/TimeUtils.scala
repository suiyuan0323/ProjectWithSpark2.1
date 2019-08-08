package org.utils

import java.util.Calendar

/**
  * @author xiaomei.wang
  * @date 2019/7/10 16:05
  * @version 1.0
  */
object TimeUtils {

  /**
    * 返回当前时间相差index个小时的整点小时的时间戳
    * 比如当前 2019-07-10 02：20：00 , index = -2 ,return 2019-07-10 00:00:00的timestamp
    * index = 2，则后推2小时  return 2019-07-10 04:00:00的timestamp
    *
    * @param index
    * @return 时间戳
    */
  def getEndTimeStamp(index: Int) = {
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.HOUR, index)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }
}
