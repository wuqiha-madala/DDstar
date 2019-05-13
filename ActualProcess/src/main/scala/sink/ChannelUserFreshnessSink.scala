package sink

import bean.ChannelUserFreshness
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName
import tools.HbaseUtils

/**
  * Created by Liutao on 2019/5/13 14:59
  */
class ChannelUserFreshnessSink extends SinkFunction[ChannelUserFreshness]{
  override def invoke(value: ChannelUserFreshness): Unit = {
    val tableName = TableName.valueOf("channel")
    val columnFamily = "info"
    val newCountColumn = "newCount"
    val oldCountColumn = "oldCount"
    val rowkey = value.getChannelID + ":" + value.getDataField
    var newCount = value.getNewCount
    var oldCount = value.getOldCount

    //查询历史数据，然后和新数据进行叠加操作
    val newCountData = HbaseUtils.getData(tableName  , rowkey , columnFamily , newCountColumn)
    val oldCountData = HbaseUtils.getData(tableName , rowkey , columnFamily , oldCountColumn)
    if(StringUtils.isNotBlank(newCountData)){
      newCount = newCount + newCountData.toLong
    }
    if(StringUtils.isNotBlank(oldCountData)){
      oldCount = oldCount + oldCountData.toLong
    }

    var map = Map[String , Long]()
    map += (newCountColumn -> newCount)
    map += (oldCountColumn -> oldCount)

    HbaseUtils.putMapData(tableName , rowkey , columnFamily , map)

  }
}
