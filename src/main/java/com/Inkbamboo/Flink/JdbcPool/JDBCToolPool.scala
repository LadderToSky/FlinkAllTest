package com.Inkbamboo.Flink.JdbcPool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable

private class JDBCToolPool {

  //第一步加载驱动类
  try{
    val driver = GetConfigTool.getProperty("jdbc.driver")
    Class.forName(driver)
  }catch{
    case e:Exception=>e.printStackTrace()
  }

  //第二步 初始化数据库连接池

  val dataSource = new mutable.ArrayStack[Connection]()

  private def initConnection: Unit ={
println("初始化连接池开始")
    try {
      val size = GetConfigTool.getInteger("jdbc.datasource.size")
      val url = GetConfigTool.getProperty("jdbc.url")
      val user = GetConfigTool.getProperty("jdbc.user")
      val password = GetConfigTool.getProperty("jdbc.password")
      for (i <- 0 until size) {
        val conn = DriverManager.getConnection(url, user, password)
        dataSource.push(conn)
      }
    } catch {
      case e:Exception =>e.printStackTrace()
    }
  }

  //第三步   获取数据库连接
  def getConnection: Connection ={
    var conn:Connection = null
    AnyRef.synchronized({
      while(dataSource.length==0){
        Thread.sleep(1000)
      }
      conn = dataSource.pop()
    }
    )
    conn
  }

  //第四步 释放数据库连接
  def returnConnection(conn:Connection): Unit ={
    dataSource.push(conn)
  }

  //执行sql查询
  def executeQuery(sql:String,params:Array[Object],callback:QueryCallback): Unit ={

    var conn :Connection= null
    var pstmt :PreparedStatement= null
    var res:ResultSet = null

    try{
      conn = getConnection
      pstmt = conn.prepareStatement(sql)
      if(params.length>0){
        for(i<-0 until params.length){
          pstmt.setObject(i+1,params(i))
        }
      }
      res = pstmt.executeQuery()
      callback.process(res)
    }catch{
      case e:Exception=>e.printStackTrace()
    }finally {
      if(conn!=null){
        returnConnection(conn)
      }
    }
  }



  //在主构造器中调用初始化连接池的方法
  initConnection
}
//结果处理的接口trait
trait QueryCallback{
  def process(rs:ResultSet)
}

//对外提供单例访问的伴生对象
object JDBCToolPool{

  private val jdbcToolPool = new JDBCToolPool

  def executeQuery(sql:String,params:Array[Object],callback:QueryCallback): Unit ={
    jdbcToolPool.executeQuery(sql,params,callback)
  }
}


//测试类
object Testjdbc{
  def main(args: Array[String]): Unit = {
    val sql = "select * from hr.departments"

    println(sql)
    val arr = Array[Object]()

    JDBCToolPool.executeQuery(sql,arr,new QueryCallback {
      override def process(rs: ResultSet): Unit = {

        val size = rs.getMetaData.getColumnCount
        println(size+"-------------")
        while(rs.next()){
          println(rs.getString(1))
        }
      }
    })
  }
}
