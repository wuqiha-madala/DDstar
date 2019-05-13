package reduce

import bean.ChannelPVUV
import org.apache.flink.api.common.functions.ReduceFunction
/**
  * Created by Liutao on 2019/5/13 14:44
  */
class ChannelPVUVReduce extends ReduceFunction[ChannelPVUV]{
  override def reduce(v1: ChannelPVUV, v2: ChannelPVUV): ChannelPVUV = {
    val channelID = v1.getChannelID
    val timeStamp = v1.getTimeStamp
    val dateField = v1.getDateField
    val userId = v1.getUserId
    val aggregateField = v1.getAggregateField
    val pv = v1.getPV + v2.getPV
    val uv = v1.getUV + v2.getUV
    val channelPVUV = new ChannelPVUV
    channelPVUV.setChannelID(channelID)
    channelPVUV.setAggregateField(aggregateField)
    channelPVUV.setDateField(dateField)
    channelPVUV.setTimeStamp(timeStamp)
    channelPVUV.setUserID(userId)
    channelPVUV.setPV(pv)
    channelPVUV.setUV(uv)
    channelPVUV
  }
}