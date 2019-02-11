package com.InkBamboo.Test

import java.io.File
import java.lang

import com.Inkbamboo.Flink.Tabledemo.HotItems
import com.Inkbamboo.Flink.Tabledemo.HotItems.ItemViewCount
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HotItems2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setMaxParallelism(1)

    val fileurl = HotItems2.getClass.getClassLoader.getResource("UserBehavior.csv")
    val filepath = Path.fromLocalFile(new File(fileurl.toURI))
    val pojoType =TypeExtractor.createTypeInfo(UserBehavior2.getClass)

    val fieldOrder = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")
    // 创建 PojoCsvInputFormat
   // val csvInput = new PojoCsvInputFormat[UserBehavior2](filepath,pojoType,fieldOrder)

    //val csvin = new
    println(pojoType.canEqual())
    //val tmpres = env.readFile(csvInput,filepath.getPath)
    /*val tmpres = env.(csvInput,filepath.getPath)
      .assignAscendingTimestamps(assign=>assign.timestamp*1000)
      .filter(assign=>assign.behavior=="pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new CountAgg2)
      .keyBy("windowEnd")
      .print()*/

    //val tmpres = env.addSource(csvInput)

    env.execute("Hot Items Job")
  }

}


case class UserBehavior2(
 var userId: Long,
// 用户ID
var itemId: Long, // 商品ID
var categoryId: Int,// 商品类目ID
var behavior: String , // 用户行为, 包括("pv", "buy", "cart", "fav")
var timestamp: Long// 行为发生的时间戳，单位秒
)

class CountAgg2 extends AggregateFunction[UserBehavior2,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior2, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class WindowResultFunction2 extends WindowFunction[Long, HotItems.ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: lang.Iterable[Long], out: Collector[HotItems.ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val count = input.iterator.next
    out.collect(ItemViewCount.of(itemId, window.getEnd, count))
  }
}