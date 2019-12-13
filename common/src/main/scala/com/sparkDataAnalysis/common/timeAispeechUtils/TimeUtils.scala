package com.sparkDataAnalysis.common.timeAispeechUtils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * @author xiaomei.wang
  * @date 2019/11/4 10:27
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
    cal.add(Calendar.HOUR_OF_DAY, index)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

  /**
    * 返回格式化的时间
    * eg dateFormat is  ''HH'' , then the result is hour
    *
    * @param timeStamp
    * @param dateFormat
    * @return
    */
  def getTimeFromStamp(timeStamp: Long, dateFormat: String) = {
    new SimpleDateFormat(dateFormat).format(new Date(timeStamp))
  }

  /**
    * 返回新的format
    * @param timeString
    * @param beforeFormat
    * @param afterFormat
    * @return
    */
  def reFormatTime(timeString: String, beforeFormat: String, afterFormat: String) = {
    val time = new SimpleDateFormat(beforeFormat)
    new SimpleDateFormat(afterFormat).format(time.parse(timeString))
  }

  /**
    * 返回当前时刻所属的整点小时的时间戳
    * 比如传入 0：30的时间戳，返回0：00的时间戳
    */
  def getIntPointHourTimestamp(timeStamp: Long): Long = {
    val cal: Calendar = Calendar.getInstance
    cal.setTime(new Date(timeStamp))
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

  def getZeroPointDayTimestamp(timeStamp: Long): Long = {
    val cal: Calendar = Calendar.getInstance
    cal.setTime(new Date(timeStamp))
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }
}
