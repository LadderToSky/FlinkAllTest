package com.InkBamboo.Test

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
object WikipediaAnalysis2 {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.183.135:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "192.168.183.135:2181")
    //设置kafka的消费组
    properties.setProperty("group.id", "test")
    //流处理使用StreamExecutionEnvironment   批处理使用ExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println(StreamExecutionEnvironment.getExecutionEnvironment)

    env.setParallelism(1)
   // val wiki = new WikipediaEditsSource()
    val wikisource = env.addSource(new WikipediaEditsSource)    //由于该类是java代码，无法隐式转换为scala代码
   val streams =  wikisource.keyBy(x=>{
      x.getUser
    })
    println("----------streams------------------")
    streams.print()
      val stream2 = streams.timeWindow(Time.seconds(30))
    println("----------stream2------------------")
      //源码翻译：将给定的fold函数应用于每个窗口。针对每个键分别调用窗口的每个求值函数。reduce函数的输出被解释为一个常规的非窗口流。
      val stream3 = stream2.fold(("-----------|",0L))((x,y)=>(y.getUser+x._1,x._2+y.getByteDiff))
      stream3.map(x=>x.toString()).print()
      /**输出到kafka中
        * kafka-topics.sh --zookeeper 192.168.183.135:2181 --topic wiki_test --replication-factor 1 --partitions 1 --create
        *
        * kafka-console-producer.sh --broker-list 192.168.183.135:9092 --topic wiki_test
        *
        * kafka-console-consumer.sh  --bootstrap-server 192.168.183.135:9092 --topic wiki_test --from-beginning
        */
      //.addSink(new FlinkKafkaProducer010[String]("wiki_test",new SimpleStringSchema(),properties))
    /**
      * 结果样例：
      * (04:2D80:C004:91EA:ED9E:E08F:A475:1380-----------,0)
      * (Zackmann08-----------,0)
      * (Funkymonkeyinthesun-----------,-1)
      * (Mzaru1-----------,947)
      * (Bot1058-----------,3)
      * (HBC AIV helperbot5-----------,-268)
      * (Colton Meltzer-----------,204)
      * (2.15.64.170-----------,2)
      * (SQLBot-----------,-182)
      */
    env.execute("wikiTest")
  }
}
