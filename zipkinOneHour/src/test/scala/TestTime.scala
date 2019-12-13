import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.utils.TimeUtils

/**
  * @author xiaomei.wang
  * @date 2019/7/10 15:43
  * @version 1.0
  */
object TestTime {
  def main(args: Array[String]): Unit = {

    testJson()

  }

  def testTime = {
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.HOUR, -1)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    println(cal.getTimeInMillis)

    val startTimeStamp = TimeUtils.getEndTimeStamp(-2)
    val endTimeStamp = startTimeStamp + 3600000
    val dayStr = new SimpleDateFormat("yyyy-MM-dd").format(startTimeStamp)
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTimeStamp)

    print(dayStr)

  }

  def testJson(): Unit = {
    val jsonTest =
      s"""{"2019-08-22":[{"beginTimestamp":1566442800000,"endTimestamp":1566446400000},       {"beginTimestamp":1566446400000,"endTimestamp":1566450000000},
{"beginTimestamp":1566450000000,"endTimestamp":1566453600000},{"beginTimestamp":1566453600000,"endTimestamp":1566457200000},
{"beginTimestamp":1566457200000,"endTimestamp":1566460800000}]}"""

    val dayStrMap = JSON.parseObject(jsonTest).getInnerMap
    val dayKeys = dayStrMap.keySet().iterator()
    while (dayKeys.hasNext) {
      val dayStr = dayKeys.next()
      val timeStampJsonArray: JSONArray = dayStrMap.get(dayStr).asInstanceOf[JSONArray]
      for (i <- 0 until  timeStampJsonArray.size()) {
        val beginTimestamp = timeStampJsonArray.getJSONObject(i).getDouble("beginTimestamp")
        val endTimestamp = timeStampJsonArray.getJSONObject(i).getDouble("endTimestamp")
        println(dayStr)
      }
    }

    println("-----")
  }
}
