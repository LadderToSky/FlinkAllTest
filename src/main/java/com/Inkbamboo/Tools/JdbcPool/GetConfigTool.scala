package com.Inkbamboo.Tools.JdbcPool

import java.util.Properties

/**
  * Created by admin2 on 2017/10/10.
  */
object GetConfigTool {
  /**
    * 由于object的构造器只在第一次执行的时候执行一次
    * 故将加载配置文件的任务放在主构造器中
    * 只会加载一次避免重复加载消耗资源
    */
  val prop = new Properties()
  try{
    val in = GetConfigTool.getClass.getClassLoader.getResourceAsStream("DB.properties")
    prop.load(in)
  }catch{
    case e:Exception=>e.printStackTrace()
  }


  def getProp():Properties={
    prop
  }
  /**
    * 获取配置文件中的对应信息
    * @param key
    * @return
    */
  def getProperty(key:String):String={
    prop.getProperty(key)
  }

  def getInteger(key:String):Integer={
    prop.getProperty(key).toInt
  }

  def getBoolean(key:String):Boolean={
    prop.getProperty(key).toBoolean
  }

  def getLong(key:String):Long={
    prop.getProperty(key).toLong
  }
}

/**
  * 测试程序
object test{
  def main(args: Array[String]): Unit = {
    println(GetConfigTool.getProperty("jdbc.url"))
  }
}*/