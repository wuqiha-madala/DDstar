package task
import `trait`.DataProcess
import bean.{ChannelUserFreshness, Message}
import map.ChannelUserFreshnessMap
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import reduce.ChannelUserFreshnessReduce
import sink.ChannelUserFreshnessSink
/**
  * Created by Liutao on 2019/5/13 15:28
  */
object ChannelUserFreshnessTask extends DataProcess{
  override def process(watermarkData: DataStream[Message]): Unit = {
    //1）：根据传递过来的水印数据解析出用户新鲜度数据 meesage ---> ChannelUserFreshness
    val mapData: DataStream[ChannelUserFreshness] = watermarkData.flatMap(new ChannelUserFreshnessMap)
    //2):数据分流操作
    val keybyData: KeyedStream[ChannelUserFreshness, String] = mapData.keyBy(line => line.getAggregateField)
    //3): 划分时间窗口
    val timeData: WindowedStream[ChannelUserFreshness, String, TimeWindow] = keybyData.timeWindow(Time.seconds(3))
    //4): 对新鲜度指标进行聚合操作
    val result: DataStream[ChannelUserFreshness] = timeData.reduce(new ChannelUserFreshnessReduce)
    //5）：将结果落地到hbase中
    result.addSink(new ChannelUserFreshnessSink)
  }
}
