package task

import `trait`.DataProcess
import bean.{ChannelPVUV, Message}
import map.ChannelPVUVMap
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import reduce.ChannelPVUVReduce
import sink.ChannelPVUVSink
/**
  * Created by Liutao on 2019/5/13 15:05
  */
object ChannelPVUVTask extends DataProcess{
  override def process(watermarkData: DataStream[Message]): Unit = {
    //根据水印数据获取PVUV实体类
    val pvuvMapData: DataStream[ChannelPVUV] = watermarkData.flatMap(new ChannelPVUVMap)
    //将数据进行分流
    val groupData: KeyedStream[ChannelPVUV, String] = pvuvMapData.keyBy(line => line.getAggregateField)
    //时间窗口划分
    val window: WindowedStream[ChannelPVUV, String, TimeWindow] = groupData.timeWindow(Time.seconds(3))
    //将数据进行聚合
    val result: DataStream[ChannelPVUV] = window.reduce(new ChannelPVUVReduce)
    result.addSink(new ChannelPVUVSink)

  }
}
