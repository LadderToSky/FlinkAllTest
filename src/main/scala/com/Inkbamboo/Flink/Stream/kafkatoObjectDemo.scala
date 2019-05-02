package com.Inkbamboo.Flink.Stream

import java.util.Properties
import net.sf.json.{JSONObject}
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

/**
  * Created By InkBamboo
  * Date: 2018/12/30 11:03
  * Calm Positive
  * Think Then Ask
  *
  * 将kafka中的数据转换成对应的java实体类对象
  * 此种转换为java对象的用途  除了添加数据的schema外，更多还需要探索
  */
object kafkatoObjectDemo extends  App {

  val senv = StreamExecutionEnvironment.getExecutionEnvironment

  val props = new Properties()
  props.setProperty("bootstrap.servers", "192.168.137.131:9092")
  //只有kafka0.8需要该配置项
  //props.setProperty("zookeeper.connect","192.168.183.133:2181")
  props.setProperty("group.id","test")

  //自定义对kafka中的数据进行处理，转换为object对象
  val kafkaschema = new FlinkKafkaConsumer010[wordcount]("realTimeDataHouse",new wordcountsxxchema,props).setStartFromLatest()

 val kafkastream =  senv.addSource(kafkaschema)

  senv.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE)
  //此处添加乱序数据中对延迟数据容忍固定时间的水位线
  kafkastream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[wordcount]{
    private val maxOutOfOrderness = 1000
    private var currentTimstamp:Long=0
    override def getCurrentWatermark: Watermark = {
      //返回新的watermark，时间为当前数据的最新时间-允许乱序的最大时间
       new Watermark(currentTimstamp-maxOutOfOrderness)
    }

    override def extractTimestamp(element: wordcount, previousElementTimestamp: Long): Long = {
        val times = element.time
      currentTimstamp = Math.max(times,currentTimstamp)
      times
    }
  }).keyBy("word")
    .timeWindow(Time.seconds(3))
    .sum("count")
    .print()


  senv.execute("kafkaschemaTest")


}

//case class wordcount(word:String,time:Long,count:Int)

/**
  * 自定义实现 kafka需要的序列化类，来指定生成kafka中数据对应的schema数据
  *
  * 改为java代码可以直接使用(wordcount)JSONObject.toBean方法转换成wordcount类型
  */
class wordcountsxxchema extends AbstractDeserializationSchema[wordcount]{

  override def deserialize(message: Array[Byte]): wordcount = {

    val str = new String(message)
    var json:JSONObject= JSONObject.fromObject("{\"word\":\"123123\",\"time\":12345678,\"count\":1}")
    try{json = JSONObject.fromObject(str)}
    catch {
      case e=>e.printStackTrace()
    }
    val word = wordcount(json.getString("word"),json.getInt("time"),json.getInt("count"))
   //val res =  JSONObject.toBean(json,wordcount.getClass).asInstanceOf[wordcount]
    //res
    word
  }
}

/*class wordcountSchema extends DeserializationSchema[wordcount]{
  override def deserialize(message: Array[Byte]): wordcount = {

    val fstconf = FSTConfiguration.getDefaultConfiguration
    fstconf.asObject(message).tostring
  }

  override def isEndOfStream(nextElement: wordcount): Boolean = {

    false
  }

  override def getProducedType: TypeInformation[wordcount] = {
    TypeInformation.of(wordcount.getClass)
  }
}*/