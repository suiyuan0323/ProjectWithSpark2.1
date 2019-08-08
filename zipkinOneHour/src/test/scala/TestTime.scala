import java.text.SimpleDateFormat
import java.util.Calendar

import org.utils.TimeUtils

/**
  * @author xiaomei.wang
  * @date 2019/7/10 15:43
  * @version 1.0
  */
object TestTime {
  def main(args: Array[String]): Unit = {

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
}
