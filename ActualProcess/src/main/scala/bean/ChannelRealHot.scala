package bean

/**
  * Created by Liutao on 2019/5/8 17:42
  */
class ChannelRealHot {
  private var channelID:String = ""
  private var count:Long = 0L

  //get方法
  def getChannelID = channelID
  def getCount = count
  //set方法
  def setChannelID(value:String) = {channelID = value}
  def setCount(value:Long) = {count = value}

  override def toString: String = s"ChannelRealHot(channelID=${channelID} , count=${count})"
}