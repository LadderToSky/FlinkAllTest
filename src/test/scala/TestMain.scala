import com.Inkbamboo.Flink.streamdemo.wordcount
import net.sf.json.JSONObject
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.joda.time.DateTime

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

  val word =JSONObject.toBean(jSONObject,wordcount.getClass)

  println("-------------------2-------------------------")
  //2. string类型转换为DateTime

  val str2= "2019-01-23T14:37:07.282+08:00"
  val dateTime = new DateTime(str2)

  println(dateTime)

}
