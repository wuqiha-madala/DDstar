package map
import bean.{ChannelRealHot, Message}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
/**
  * Created by Liutao on 2019/5/8 17:41
  */
class RealHotMap extends FlatMapFunction[Message , ChannelRealHot]{
  override def flatMap(t: Message, out: Collector[ChannelRealHot]): Unit = {
    val channelID = t.userScan.channelID
    val count = t.count
    val channelRealHot = new ChannelRealHot
    channelRealHot.setChannelID(channelID)
    channelRealHot.setCount(count)
    out.collect(channelRealHot)
  }
}