package com.Inkbamboo.Flink.stream

import java.io.File
import java.sql.Timestamp
import java.util
import java.util.Comparator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**scala实现
  ****** flink特刊中给出的示例代码
  * 如何计算实时热门商品以及TopN实现
  * https://github.com/wuchong/my-flink-project/blob/master/src/main/java/myflink/HotItems.java
  */
object HotItems2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    // 告诉系统按照 EventTime 处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //并发度并不影响程序运行，但是因为数据量等原因，该代码的并发度>1故导致设置并发度为1时程序报错。
    //env.setMaxParallelism(1)

    // UserBehavior.csv 的本地文件路径, 在 resources 目录下
    val fileurl = HotItems2.getClass.getClassLoader.getResource("UserBehavior.csv")
    val filepath = Path.fromLocalFile(new File(fileurl.toURI))

    // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
    val pojoType =TypeExtractor.createTypeInfo(classOf[UserBehavior2]).asInstanceOf[PojoTypeInfo[UserBehavior2]]
    // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
    val fieldOrder = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")

    // 创建 PojoCsvInputFormat
    val csvInput = new PojoCsvInputFormat[UserBehavior2](filepath,pojoType,fieldOrder)//.asInstanceOf[PojoTypeInfo[UserBehavior2]]

    //val tmpres = env.readFile(csvInput,filepath.getPath)
    env
      // 创建数据源，得到 UserBehavior 类型的 DataStream
      .createInput(csvInput)(pojoType)
      // 抽取出时间和生成 watermark
      // 原始数据单位秒，将其转成毫秒
      .assignAscendingTimestamps(assign=>assign.timestamp*1000)
      // 过滤出只有点击的数据
      .filter(assign=>assign.behavior=="pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(30),Time.minutes(5))
      .aggregate(new CountAgg2,new WindowResultFunction2)
      .keyBy("windowEnd")
      .process(new keyProcessTopN(3))
      .print()

    //val tmpres = env.addSource(csvInput)

    env.execute("Hot Items Job")
  }

}

//定义为pojo类
/**
  *
  * @param userId     用户ID
  * @param itemId     商品ID
  * @param categoryId 商品类目ID
  * @param behavior   用户行为, 包括("pv", "buy", "cart", "fav")
  * @param timestamp   行为发生的时间戳，单位秒
  *
  */
 class UserBehavior2(
 var userId: Long,
// 用户ID
var itemId: Long, // 商品ID
var categoryId: Int,// 商品类目ID
var behavior: String , // 用户行为, 包括("pv", "buy", "cart", "fav")
var timestamp: Long// 行为发生的时间戳，单位
){
  //此处主要用于flink将该类识别为pojo类
  def this(){
    this(0,0,0,null,0)
  }
}

/** 商品点击量(窗口操作的输出类型) */
object ItemViewCount2 {
  def of(itemId: Long, windowEnd: Long, viewCount: Long):ItemViewCount2 = {
    val result = new ItemViewCount2
    result.itemId = itemId
    result.windowEnd = windowEnd
    result.viewCount = viewCount
    result
  }
}

class ItemViewCount2 {
  var itemId = 0L // 商品ID

  var windowEnd = 0L // 窗口结束时间戳

  var viewCount = 0L // 商品的点击量

   override def toString: String = {
    ""+itemId.toString+"   "+windowEnd+"    "+viewCount
  }
}


/**
  * COUNT 统计的聚合函数实现，每出现一条记录加一
  */
class CountAgg2 extends AggregateFunction[UserBehavior2,Long,Long]{
  override def createAccumulator() = 0L

  override def add(value: UserBehavior2, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

/** 用于输出窗口的结果
  *
  *
  * */
class WindowResultFunction2 extends WindowFunction[Long, ItemViewCount2, Tuple, TimeWindow]{

  /**
    *
    * @param key // 窗口的主键，即 itemId
    * @param window // 窗口
    * @param input // 聚合函数的结果，即 count 值
    * @param out // 输出类型为 ItemViewCount
    *
    */
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount2]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0   //将key值解析出来
    val count = input.iterator.next    //获取key对应的计算值
    out.collect(ItemViewCount2.of(itemId, window.getEnd, count))
  }
}

/**
  * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
  * */
class keyProcessTopN(topSize:Int) extends KeyedProcessFunction[Tuple,ItemViewCount2,String]{

  // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
   var itemState:ListState[ItemViewCount2]=null

  override def open(parameters: Configuration): Unit = {
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount2]("itemState-state", classOf[ItemViewCount2])
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }


  override def processElement(value: ItemViewCount2, ctx: KeyedProcessFunction[Tuple, ItemViewCount2, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都保存到状态中
    itemState.add(value)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount2, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 获取收到的所有商品点击量
    val allItems = new util.ArrayList[ItemViewCount2]()
    for(item<-itemState.get()){
      allItems.add(item)
    }
    // 提前清除状态中的数据，释放空间
    itemState.clear()

    allItems.sort(new Comparator[ItemViewCount2]{
      override def compare(o1: ItemViewCount2, o2: ItemViewCount2): Int = {
        // 按照点击量从大到小排序
        (o2.viewCount-o1.viewCount).asInstanceOf[Int]     //逆序排列
      }
    })

    // 将排名信息格式化成 String, 便于打印
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n")
    var i=0
    for (item<-allItems) {
      if(i<topSize) result.append("No").append(i).append(":")
        .append("  商品ID=").append(item.itemId)
        .append("  浏览量=").append(item.viewCount)
        .append("\n")
      i+=1
    }

    result.append("====================================\n\n")

    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}