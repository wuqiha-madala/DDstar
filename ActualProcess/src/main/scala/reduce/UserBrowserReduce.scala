package reduce
import bean.UserBrowser
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by Liutao on 2019/5/13 14:46
  */
class UserBrowserReduce extends ReduceFunction[UserBrowser]{
  override def reduce(before: UserBrowser, after: UserBrowser): UserBrowser = {
    val dataField = before.getDataField
    val browser = before.getBrowser
    val tiemStamp = before.getTimeStamp

    val count = before.getCount + after.getCount
    val newCount = before.getNewCount + after.getNewCount
    val oldCount = before.getOldCount + after.getOldCount

    val userBrowser = new UserBrowser
    userBrowser.setDataField(dataField)
    userBrowser.setOldCount(oldCount)
    userBrowser.setNewCount(newCount)
    userBrowser.setCount(count)
    userBrowser.setBrowser(browser)
    userBrowser.setTimeStamp(tiemStamp)
    userBrowser
  }
}