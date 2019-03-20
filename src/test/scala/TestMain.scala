
import net.sf.json.JSONObject
import org.joda.time.DateTime
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created By InkBamboo
  * Date: 2018/12/30 16:59
  * Calm Positive
  * Think Then Ask
  */
object TestMain extends App {

  //1. json字符串封装成类对象
  val str = "{\"word\":\"a3bcd\",\"time\":1546146333716,\"count\":1}"
  val jSONObject = JSONObject.fromObject(str)

  val word =JSONObject.toBean(jSONObject,wordcount2.getClass)

  println("-------------------2-------------------------")
  //2. string类型转换为DateTime

  val str2= "2019-01-23T14:37:07.282+08:00"
  val dateTime = new DateTime(str2)

  println(dateTime)

}
case class wordcount2(word:String,time:Long,count:Int)


//maxby取最大值
object StreamTest extends App {

  val senv = StreamExecutionEnvironment.getExecutionEnvironment
  senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 // senv.setParallelism(1)
 val element =  senv.fromElements(new Tuple3("a",2,1511658000),new Tuple3("b",4,1511658000),new Tuple3("a",5,1511658000),new Tuple3("b",2,1511658000))
  element
      .assignAscendingTimestamps(x=>x._3)
    .keyBy(0)
    .timeWindow(Time.seconds(10))
    .maxBy(1)
    //.sum(1)
    .print()

  senv.execute("testMaxBy")
}