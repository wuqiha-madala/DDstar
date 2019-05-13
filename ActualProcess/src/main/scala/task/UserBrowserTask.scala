package task

import `trait`.DataProcess
import bean.{Message, UserBrowser}
import map.UserBrowserMap
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import reduce.UserBrowserReduce
import sink.UserBrowserSink

/**
  * Created by Liutao on 2019/5/13 15:25
  */
object UserBrowserTask extends DataProcess{
  override def process(watermarkData: DataStream[Message]): Unit = {
    //1):根据水印数据转换出：用户浏览器数据
    val mapData: DataStream[UserBrowser] = watermarkData.flatMap(new UserBrowserMap)
    //2）：数据分流
    val keyBydata: KeyedStream[UserBrowser, String] = mapData.keyBy(line => line.getDataField)
    //3):时间窗口划分
    val window: WindowedStream[UserBrowser, String, TimeWindow] = keyBydata.timeWindow(Time.seconds(3))
    //4):指标聚合
    val result = window.reduce(new UserBrowserReduce)
    //5）：数据指标落地
    result.addSink(new UserBrowserSink)
  }
}
