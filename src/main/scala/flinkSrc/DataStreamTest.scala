package flinkSrc

import java.io.File

import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * Created By InkBamboo
  * Date: 2019/3/19 14:49
  * Calm Positive
  * Think Then Ask
  *
  * DataStream功能测试验证代码
  */
object DataStreamTest extends App {

  val senv = StreamExecutionEnvironment.getExecutionEnvironment

 // val denv = ExecutionEnvironment.getExecutionEnvironment
 // val element =  senv.fromElements(new Tuple3("a",2,1511658000),new Tuple3("b",4,1511658000),new Tuple3("a",5,1511658000),new Tuple3("b",2,1511658000))

//数据源是用户消费行为数据
  val fileurl = DataStreamTest.getClass.getClassLoader.getResource("UserBehavior.csv")
  val filepath = Path.fromLocalFile(new File(fileurl.toURI))

  //构建数据需要的typeinfomation信息
  val pojoType = TypeExtractor.createTypeInfo(classOf[UserBehavior3]).asInstanceOf[PojoTypeInfo[UserBehavior3]]
  val fieldOrder = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")
  val csvinput = new PojoCsvInputFormat[UserBehavior3](filepath,pojoType,fieldOrder) // .isSkippingFirstLineAsHeader  设置跳过表头(表头是类名)
  /**
    * 构建数据源可以从下往上推。
    * 从createInput往上推需要什么参数，之后构建需要的参数
    */
  val datastream = senv.createInput(csvinput)

  //val dataset = denv.createInput(csvinput)

  /*************************************************************************
    * Stream operator测试
    ************************************************************************/
  datastream
    //为数据流中的元素分配时间戳，并定期创建watermark，以指示事件时间进度。
    //时间是秒级别的转换为毫秒级别
    .assignAscendingTimestamps(x=>x.timestamp*1000)
    //根据用户动作分组
    .keyBy(x=>x.behavior)
    .timeWindow(Time.minutes(30),Time.minutes(5))
    //对每个key的分组进行reduce处理
    .reduce((x,y)=>new UserBehavior3(x.userId,y.itemId,y.categoryId,x.behavior,(x.timestamp+y.timestamp)/2))


}

//pojo
class UserBehavior3(
                     var userId: Long,
                     // 用户ID
                     var itemId: Long, // 商品ID
                     var categoryId: Int,// 商品类目ID
                     var behavior: String , // 用户行为, 包括("pv", "buy", "cart", "fav")
                     var timestamp: Long// 行为发生的时间戳，单位
                   ){
  //无参构造器
  def this(){
    this(0,0,0,null,0)
  }
}
