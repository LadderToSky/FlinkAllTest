package com.Inkbamboo.Flink.Stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.LoggerFactory

import scala.util.Random
/**
  * Created By InkBamboo
  * Date: 2018/12/24 15:14
  * Calm Positive
  * Think Then Ask
  *
  * watermark水位线测试
  *
  *！！！！！！例子有问题，正常来说超过3.5s,的数据应该会被丢弃，但是例子中并没有
  *     上述说法存在问题：
  *           watermark官网给出的文档中分为很多种，只有其中的 Assigners allowing a fixed amount of lateness  是支持数据延迟到达的watermark
  *           Flink provides the BoundedOutOfOrdernessTimestampExtractor which takes as an argument the maxOutOfOrderness, i.e. the maximum amount of time an element is allowed to be late before being ignored when computing the final result for the given window
  *           https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/event_timestamp_extractors.html
  *
  * flink内部的三种事件时间
  * 一个摄取时间：到达Flink的时间
  * 一个程序时间：到达算子时间
  * 一个外部系统报文的时间：报文消息自己带的那个时间
  */
object watermarkDemo extends  App {

  val log =LoggerFactory.getLogger(watermarkDemo.getClass)
  val senv = StreamExecutionEnvironment.getExecutionEnvironment
  //设置
  senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  senv.setParallelism(1)
  senv.enableCheckpointing(10000)
  //设置水位线周期性的产生时间为5秒
  senv.getConfig.setAutoWatermarkInterval(5000)
  //val kafkaStream = senv.socketTextStream("localhost",999)

  val prop = new Properties()
  prop.setProperty("bootstrap.servers", "192.168.183.133:9092")
 // prop.setProperty("zookeeper.connect", "192.168.183.133:2181")
  prop.setProperty("group.id", "test")

  //收集kafka中的数据
  val kafkaConsumer010 = new FlinkKafkaConsumer010[String]("wiki_test",new SimpleStringSchema(),prop)
                              .setStartFromEarliest()
  val kafkaStream  = senv.addSource(kafkaConsumer010)


//  kafkaStream.timeWindowAll(Time.seconds(5))
//    .allowedLateness(Time.seconds(2))   //允许数据延迟到2秒

  val res = kafkaStream
    .flatMap(x=>x.split(" "))
    res
    .map(x=>new wordcount(x,System.currentTimeMillis()-Random.nextInt(50000),1))
    //此处按照官方文档设置watermark相较于最新数据时间提前3秒，也就是说在时间窗口中允许乱序数据晚到3秒。
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[wordcount](Time.seconds(3)){
      //此方法是指明获得的数据源中哪个字段用于时间字段，并转换为对应的long类型返回
      override def extractTimestamp(element: wordcount): Long = {
        println("---------------timestamp------"+element.time)
        element.time
      }
    })
   /* //设置水位线对应使用的数据流中的时间戳
   .assignAscendingTimestamps(x=>x.time)
    //为数据流中的元素分配时间戳，并周期性地创建水印来指示事件时间进度。
    .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[wordcount] {

    //设置乱序数据中数据允许的最大乱序时间（乱序数据距离最新数据时间差，超出此时间差的数据将被丢弃）
    val maxoutofordertimes:Long = 3500//3.5second
    //extractedTimestamp 是在下面的extractTimestamp 拿到的。在调用该方法前会先调用extractTimestamp方法
    //判断当前的时间戳是否大于之前的水位线，如果小于就返回空，
    //如果大于当前水位线，则重新设置水位线
    override def checkAndGetNextWatermark(lastElement: wordcount, extractedTimestamp: Long): Watermark = {
      val prewatermark = Watermark.MAX_WATERMARK
      var cuwatermark:Watermark = null
      if(prewatermark!=null && extractedTimestamp>prewatermark.getTimestamp)
        cuwatermark =new Watermark(extractedTimestamp-maxoutofordertimes)
      cuwatermark
    }

    //获取当前元素的产生时间戳  ，用于checkAndGetNextWatermark中使用
    override def extractTimestamp(element: wordcount, previousElementTimestamp: Long): Long = {
      element.time
    }
  }).keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
*/
    log.info("------output_info---")
  res.print()

  senv.execute("watermarkDemo")
}

class wordcount(var word:String,var time:Long,var count:Int){
  def this(){
    this(null,0,0)
  }
}

