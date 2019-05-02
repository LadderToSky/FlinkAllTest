package com.InkBamboo.Test

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * Created By InkBamboo
  * Date: 2019/4/2 20:51
  * Calm Positive
  * Think Then Ask
  */
class DataSource extends RichSourceFunction[Tuple2[String,Int]]{
  var running = false
  override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {

    while(running){
      Thread.sleep((getRuntimeContext.getIndexOfThisSubtask+5)*1000+500)

    }
  }

  override def cancel(): Unit = {

  }
}
object Datastream_flinkChina4 extends App {

}
