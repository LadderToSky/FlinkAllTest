package com.Inkbamboo.Flink.Table.stream
import java.io.File

import com.Inkbamboo.Flink.Stream.HotItems2_Scala
import com.Inkbamboo.beans.UserBehavior
import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Author: inkbamboo
  * Date:   2019/9/1 21:34
  *
  * Think Twice, Code Once! 
  *
  * Desc:   table针对实时数据注册成表,并添加时间字段设置；以tableApi的操作进行window数据处理
  */
object TableStreamWindowTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val tenv = TableEnvironment.getTableEnvironment(env)
  // 告诉系统按照 EventTime 处理
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //并发度并不影响程序运行，但是因为数据量等原因，该代码的并发度>1故导致设置并发度为1时程序报错。
  //env.setMaxParallelism(1)

  // UserBehavior.csv 的本地文件路径, 在 resources 目录下
  val fileurl = HotItems2_Scala.getClass.getClassLoader.getResource("UserBehavior.csv")
  val filepath = Path.fromLocalFile(new File(fileurl.toURI))

  // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
  val pojoType =TypeExtractor.createTypeInfo(classOf[UserBehavior]).asInstanceOf[PojoTypeInfo[UserBehavior]]
  // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
  val fieldOrder = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")

  // 创建 PojoCsvInputFormat
  val csvInput = new PojoCsvInputFormat[UserBehavior](filepath,pojoType,fieldOrder)//.asInstanceOf[PojoTypeInfo[UserBehavior]]

  //调用Datastream API的assignAscendingTimestamps指定EventTime和watermark信息，并在Datastream中将第一个字段提取出来并指定为EventTime字段
  val watermark = env
    // 创建数据源，得到 UserBehavior 类型的 DataStream
    .createInput(csvInput)//(pojoType)
    // 抽取出时间和生成 watermark
    // 原始数据单位秒，将其转成毫秒
    .assignAscendingTimestamps(assign=>assign.timestamp*1000)

  /**
    * tableApI相关操作
    */
  //在table schema末尾使用'timestamp.rowtime 定义EventTime字段
  //********** 系统会从TableEnvironment中获取EventTime的信息
  val table = tenv.fromDataStream(watermark,'userId,'itemId,'categoryId,'behavior,'timestamp.rowtime)
  //设置滚动窗口时间为5分钟，并且时间类型为eventtime
  //设置 时间窗口的名称为eventwindow
  val stbl = table.window(Tumble over 5.minutes on 'timestamp as 'eventwindow)
    //窗口设置之后必须使用groupby，如果不指定分组的id，则与globalwindow一样，所有的数据全部发送到一个task上
    .groupBy('eventwindow,'behavior)
    //设置需要查询的字段,以及计算方式，获取窗口的开始时间，结束时间(含窗口区间的上界)，结束时间(不含窗口区间上界)
    .select('behavior,'itemId.count,'eventwindow.start,'eventwindow.end,'eventwindow.rowtime)
      .toAppendStream[Row].print()

println("-------------------------------")
  //当在第一个字段上定义'timestamp.rowtime时，
  // ********** 系统使用Datastream中对应字段作为EventTime字段
  tenv.fromDataStream(watermark,'timestamp.rowtime,'userId,'itemId,'categoryId,'behavior)
      .window(Tumble over 10.minutes on 'timestamp as 'eventwindow)
      .groupBy('eventwindow,'categoryId)
      .select('categoryId,'itemId.count,'eventwindow.start,'eventwindow.end,'eventwindow.rowtime)
      .toAppendStream[Row].print()





  env.execute("tableWindowTest")

}
