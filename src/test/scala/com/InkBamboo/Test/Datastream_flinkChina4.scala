package com.InkBamboo.Test

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.util.Random

/**
  * Created By InkBamboo
  * Date: 2019/4/2 20:51
  * Calm Positive
  * Think Then Ask
  *
  * 钉钉flink官网群 flink入门教程4 DatastreamAPI
  */
class DataSource extends RichSourceFunction[Tuple2[String,Int]]{
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[(String, Int)]): Unit = {

    while(running){
      //Thread.sleep((getRuntimeContext.getIndexOfThisSubtask+5)*1000+500)
      val tupe2 = ((Random.nextInt(25)+65).asInstanceOf[Char]+"",Random.nextInt(20))
      println("-tuple2:"+tupe2)
      sourceContext.collect(tupe2)
    }
  }

  override def cancel(): Unit = {
running = false
  }
}
object Datastream_flinkChina4 extends App {

  val envstream = StreamExecutionEnvironment.getExecutionEnvironment
  envstream.setParallelism(1)
  val demoSource = envstream.addSource(new DataSource)
val rest = demoSource.keyBy(0).sum(1).keyBy(new KeySelector[(String,Int),String](){
  override def getKey(in: (String, Int)): String = {
    //返回空会将所有的数据集中到一个task上造成数据倾斜，实际工作中不会这么做
    ""
  }
})
  //fold操作类似reduce操作，但需要一个初始值
  .fold(new mutable.HashMap[String,Int](),new FoldFunction[Tuple2[String,Int],mutable.Map[String,Int]]() {
  //t 输入数据和输出数据的类型，
  //o 算子传入的数据的类型，包含需要处理的数据
  override def fold(t: mutable.Map[String, Int], o: (String, Int)): mutable.Map[String, Int] = {
    t.put(o._1,o._2)
    t
  }
})
  //rest.print()

  rest.addSink(new SinkFunction[mutable.Map[String,Int]](){
  override def invoke(value: mutable.Map[String, Int], context: SinkFunction.Context[_]): Unit = {
    println("===================="+value.values.toStream.map(x=>x.toInt).sum)
  }
})

  //demoSource.map(x=>(x._1,x._2+10)).print()

  envstream.execute("demoTest")
}
