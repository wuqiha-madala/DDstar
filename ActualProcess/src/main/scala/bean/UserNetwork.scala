package bean

/**
  * Created by Liutao on 2019/5/13 14:38
  */
class UserNetwork {
  private var network:String = null
  private var count:Long = 0L
  private var newCount:Long = 0L
  private var oldCount:Long = 0L
  private var timeStamp:Long = 0L
  private var dataField:String = null

  def getNetwork = network
  def getCount = count
  def getNewCount = newCount
  def getOldCount = oldCount
  def getTimeStamp = timeStamp
  def getDataField = dataField


  def setNetwork(value:String) = {network = value}
  def setCount(value:Long) = {count = value}
  def setNewCount(value:Long) = {newCount = value}
  def setOldCount(value:Long) = {oldCount = value}
  def setTimeStamp(value:Long) = {timeStamp = value}
  def setDataField(value:String) = {dataField = value}

}
