package com.Inkbamboo.Flink.Stream.demo

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
object SocketWindowWordCount {

  def main(args: Array[String]) : Unit = {

    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123)
    val path = new Path("")
    val format = new TextInputFormat(path)
    //设置文件编码格式

    format.setCharsetName("UTF-8")
    //周期性的监控文件作为flink的数据源
    env.readFile(format,"filepath",FileProcessingMode.PROCESS_CONTINUOUSLY,10)
    // get input data by connecting to the socket
    val text = env.socketTextStream("hadoop", port, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split(" ") }
      .map { w => new WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(1), Time.seconds(3))
      .sum("count")
    // print the results with a single thread, rather than in parallel
    windowCounts.print()//.setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  //pojo
  //case class存在gson.fromJson(json,Classof[WordWithCount]) 部分转换问题
  class WordWithCount(word: String, count: Long){
    def this(){
      this(null,0L)
    }
  }
}
