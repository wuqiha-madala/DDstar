package task

import `trait`.DataProcess
import bean.{ChannelRealHot, Message}
import map.RealHotMap
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import reduce.ChannelRealHotReduce
import sink.ChannelRealHotSink
/**
  * Created by Liutao on 2019/5/9 10:17
  */
object ChannelRealHotTask extends DataProcess{
  override def process(watermarkData: DataStream[Message]): Unit = {

    //将实时解析出的数据转换成频道热点实体类
    val channelRealHot: DataStream[ChannelRealHot] = watermarkData.flatMap(new RealHotMap)
    //分流（分组）
    val keyByData: KeyedStream[ChannelRealHot, String] = channelRealHot.keyBy(line => line.getChannelID)
    //时间窗口的划分
    val window: WindowedStream[ChannelRealHot, String, TimeWindow] = keyByData.timeWindow(Time.seconds(3))
    //对频道的点击数进行聚合操作
    val reduceData: DataStream[ChannelRealHot] = window.reduce(new ChannelRealHotReduce)
    print("收集到的对频道的点击"+reduceData.map(line=>line.getChannelID+line.getCount))
    //将结果数据进行落地操作--->hbase
    reduceData.addSink(new ChannelRealHotSink)
  }
}