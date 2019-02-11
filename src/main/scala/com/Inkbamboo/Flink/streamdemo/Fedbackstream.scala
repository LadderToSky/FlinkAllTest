package com.Inkbamboo.Flink.streamdemo

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.JavaConverters._

/**
  * 递归迭代算子
  */
object Fedbackstream extends App {

    val streamenv = StreamExecutionEnvironment.getExecutionEnvironment
  streamenv.setParallelism(1)
  val someone:DataStream[Long] = streamenv.generateSequence(0,1000)
  //是一个迭代计算的过程，iterate内部，自定义逻辑将数据切分成两部分，(fedback,output)
  // fedbak 会再次回调到iterate中，作为新一轮的输入参数，继续计算，output的数据会最为输出数据，进入下阶段的计算中。
  val splitZero = someone.iterate(iter=>{
    val num = iter.map(v=>v - 2)
    val lessZero = num.filter(_<0)
    val moreZero = num.filter(_>=0)
    (moreZero,lessZero)
  })
  /**结果展示
    * -1
    * -2
    * -1
    * -2
    * -1
    * -2
    */
  //splitZero.print()

  val myoutput = DataStreamUtils.collect(splitZero.javaStream).asScala

  myoutput.foreach(println(_))
  streamenv.execute("FedBackstream")
}
