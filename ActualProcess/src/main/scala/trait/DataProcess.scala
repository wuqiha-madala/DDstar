package `trait`
import bean.Message
import org.apache.flink.streaming.api.scala.DataStream
/**
  * Created by Liutao on 2019/5/9 10:19
  */
trait DataProcess {
  def process(watermarkData:DataStream[Message])
}
