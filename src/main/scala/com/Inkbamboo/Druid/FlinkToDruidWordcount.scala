package com.Inkbamboo.Druid

import java.util.Properties

import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid._
import com.metamx.tranquility.flink.{BeamFactory, BeamSink}
import com.metamx.tranquility.typeclass.Timestamper
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.QueryGranularities
import io.druid.query.aggregation.LongSumAggregatorFactory
import net.sf.json.JSONObject
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._
import org.joda.time.{DateTime, Period}
import org.scala_tools.time.Imports.Period
import org.joda.time._
/**
  * Created By InkBamboo
  * Date: 2019/1/10 18:24
  * Calm Positive
  * Think Then Ask
  *
  * flink数据写入druid的测试例子，来自官网给出的。流程已经没有问题，并且已经测试通过。
  *
  * 注意测试细节需要在深入了解druid的情况下再测试。
  *
  * 测试通过数据已经写进到druid中
  *
  * ***需要详细解析内部参数的配置
  *
  *
  * 1.分钟级集合操作，不再该分钟内的数据被丢弃，并且如果创建task时，数据时间已经不再该分钟级别内，task不创建并且数据被丢弃。
  * 2. DruidBeams.builder中构建的时间，不知道作用是什么？？
  */
object FlinkToDruidWordcount extends App {


  val senv = StreamExecutionEnvironment.getExecutionEnvironment

  senv.setParallelism(5)
  val props = new Properties()
  props.setProperty("bootstrap.servers", "192.168.183.133:9092")
  //只有kafka0.8需要该配置项
  //props.setProperty("zookeeper.connect","192.168.183.133:2181")
  props.setProperty("group.id","test")

  //自定义对kafka中的数据进行处理，转换为object对象
  val kafkaschema = new FlinkKafkaConsumer010[wordcount2]("wiki_test",new wordcountschema,props).setStartFromLatest()

  val kafkastream =  senv.addSource(kafkaschema)

  kafkastream.print()

  //此处添加乱序数据中对延迟数据容忍固定时间的水位线
  val res = kafkastream
   /* .keyBy("word")
    .sum("count")
*/
  val zkConnect = "192.168.183.133:2181"
  val sink = new BeamSink[wordcount2](new SimpleEventBeamFactory2(zkConnect))

  println("------------------------------------------")
  res.print()
  res.addSink(sink)


  senv.execute("kafkaschemaTest")

}


class SimpleEventBeamFactory2(zkurl:String) extends BeamFactory[wordcount2]
{

  lazy val makeBeam: Beam[wordcount2] = {

    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      zkurl,//"localhost:2181",zookeeper地址
      //重试之间的时间间隔，重试的最大时间间隔，重试次数
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    //配置参数：http://druid.io/docs/latest/configuration/index.html

    //下面配置的参数用于生成tranquality.json文件，故可以和json文件中的元素字段对应配置
    //indexService   区分节点类型：初步了解接单类型有 druid/overlord  ,druid/coordinator,druid/broker ,druid/middleManager,druid/historical
   //****对应json:druid.selectors.indexing.serviceName
    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.==The druid.service name of the indexing service Overlord node. To start the Overlord with a different name, set it with this property.
    //discovery znode   druid在zk上存储的路径
    //****对应json:druid.discovery.curator.path
    val discoveryPath = "/druid/discovery" // Your overlord's druid.discovery.curator.path  ===Services announce themselves under this ZooKeeper path.  zookeepr内部存储druid数据的路径
    //****对应json:dataSource
    val dataSource = "foo254"   //Provide the location of your Druid dataSource    类似于数据库中的表，指定一个druid中的对应的数据块
    //****对应json:dimensions
    val dimensions = IndexedSeq("word")   //呈现的列名,可以有多个
    //****对应json:spatialDimensions
    val spatialDimensions = Nil
    //****对应json: metricsSpec
    val aggregators = Seq(new LongSumAggregatorFactory("count2", "count"))   //进行汇聚的列baz

    // Expects simpleEvent.timestamp to return a Joda DateTime object.
    //下面的参数配置构成了tranquality使用的json数据的全部内容
    DruidBeams
      //builder内部的eventtype只支持map类型，否则会报警告：Cannot partition object of class[class com.Inkbamboo.Druid.wordcount2] by time and dimensions. Consider implementing a Partitioner.
      //并且无法创建成功task任务
      //修改为map？？？？？？？
      .builder[wordcount2]()(new Timestamper[wordcount2] {
      override def timestamp(a: wordcount2): DateTime = {
        val timestamp = new DateTime(a.timestamp)
        println(s"------timestamp-------${timestamp}")
        new DateTime(timestamp)
      }
    })
      .curator(curator)
      .discoveryPath(discoveryPath)   //discovery znode
      .location(DruidLocation.create(indexService, dataSource))   //*********数据源信息
//      .druidBeamConfig(new DruidBeamConfig(
//      //以下为默认参数具体需要详细了解
//      firehoseGracePeriod= new Period("PT5M"),
//      firehoseQuietPeriod = new Period("PT1M"),
//    firehoseRetryPeriod = new Period("PT1M"),
//    firehoseChunkSize = 1000,
//    randomizeTaskId= false,
//    indexRetryPeriod= new Period("PT1M"),
//    firehoseBufferSize = 100000,
//    overlordLocator = OverlordLocator.Curator,
//      //****对应json:druidBeam.taskLocator
//    taskLocator = TaskLocator.Curator,
//      //****对应json:druidBeam.overlordPollPeriod
//    overlordPollPeriod= new Period("PT5S")
//    ))
      //***********************DruidBeamConfig  设置与druid任务通信的参数,全部有默认值
      //.eventTimestamped(x=>new DateTime())
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.MINUTE))   //聚合操作
      .tuning(   //**************控制druidtask何时以及如何创建
      ClusteredBeamTuning(
        //****对应json:segmentGranularity
        segmentGranularity = Granularity.MINUTE,
        warmingPeriod=new Period("PT0M"), //
        //****对应json:windowPeriod
        windowPeriod = new Period("PT1M"),  //窗口期10分钟
        partitions = 1,
        replicants = 1,
        minSegmentsPerBeam=2,    //每个Beam最少多少个segment
        maxSegmentsPerBeam=10    //每个Beam最多多少个segment
      )
    )
      //设置数据中时间字段以及各式，missvalue针对数据源中没有时间字段的数据 进行设置的默认时间参数(默认为当前时间)
      .timestampSpec(new TimestampSpec("timestamp","auto",null))
      .buildBeam()
  }
}



/**
  * 自定义实现 kafka需要的序列化类，来指定生成kafka中数据对应的schema数据
  *
  * 改为java代码可以直接使用(wordcount)JSONObject.toBean方法转换成wordcount类型
  */
class wordcountschema extends AbstractDeserializationSchema[wordcount2]{

  override def deserialize(message: Array[Byte]): wordcount2 = {

    val str = new String(message)
    var json:JSONObject= JSONObject.fromObject(str)
    try{json = JSONObject.fromObject(str)}
    catch {
      case e=>e.printStackTrace()
    }
    val word = wordcount2(json.getString("word"),json.getString("timestamp"),json.getInt("count"))
    //word.time = new DateTime().toString
    //val res =  JSONObject.toBean(json,wordcount2.getClass).asInstanceOf[wordcount2]
    //res
    word
  }
}
//{"word":"asdasfa","timestamp":"asdasdf","count":1}
case class wordcount2(word:String,var timestamp:String,count:Int)


