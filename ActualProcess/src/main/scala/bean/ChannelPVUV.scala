package bean

/**
  * Created by Liutao on 2019/5/13 14:33
  */
class ChannelPVUV {
  private var channelID:String = null
  private var userID:String = null
  private var pv:Long = 0L
  private var uv:Long = 0L
  private var timeStamp:Long = 0L
  private var dateField:String = null
  private var aggregateField:String =null

  def getChannelID = channelID
  def getUserId = userID
  def getPV = pv
  def getUV = uv
  def getTimeStamp = timeStamp
  def getDateField = dateField
  def getAggregateField = aggregateField

  def setChannelID(value:String) = {channelID = value}
  def setUserID(value:String) = {userID = value}
  def setPV(value:Long) = {pv = value}
  def setUV(value:Long) = {uv = value}
  def setTimeStamp(value:Long) = {timeStamp = value}
  def setDateField(value:String) = {dateField = value}
  def setAggregateField(value:String) = {aggregateField = value}
}
