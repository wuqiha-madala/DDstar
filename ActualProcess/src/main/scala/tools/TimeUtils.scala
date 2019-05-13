package tools

import java.util.Date

import org.apache.commons.lang.time.FastDateFormat

/**
  * Created by Liutao on 2019/5/9 10:05
  */
object TimeUtils {
  def getData(timeStamp: Long, format: String): String = {
    val time = new Date(timeStamp)
    val instance = FastDateFormat.getInstance(format)
    val formatResult = instance.format(time)
    formatResult
  }
}
