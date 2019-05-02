package com.InkBamboo.Test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * Created By InkBamboo
  * Date: 2019/4/16 21:35
  * Calm Positive
  * Think Then Ask
  */
object WriteToHdfs extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment
  /**
    * user_id     用户ID 整数类型，加密后的用户ID
    * pro_id      商品ID 整数类型，加密后的商品ID
    * pro_type_id 商品类目ID 整数类型，加密后的商品所属类目ID
    * act_type    行为类型 字符串，枚举类型，包括('pv', 'buy', 'cart', 'fav')
    * timestamp   时间戳 行为发生的时间戳，单位秒
    */
    //read csv格式数据并指定需要的数据列(剔除第2列数据)，并封装到pojo类中
  val csvds = env.readCsvFile[csvpojo]("src/main/resources/UserBehavior.csv",
    includedFields = Array(0,1,
      //2,
      3,4))
    //,pojoFields = Array("user_id","pro_id","pro_type_id","act_type","timestamp"))

csvds.print()
env.execute("read_csv文件")
}

case class csvpojo(user_id:String,
                   //pro_id:String,
                   pro_type_id:String,act_type:String,timestamp:Long){
  def this(){
    this(null,
      //null,
      null,null,-1)
  }
}
