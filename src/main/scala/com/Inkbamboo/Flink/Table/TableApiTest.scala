package com.Inkbamboo.Flink.Table

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._

/**
  * Created By InkBamboo
  * Date: 2018/12/17 10:41
  * Calm Positive
  * Think Then Ask
  *
  * desc:  tableAPI源码中example样例测试
  */
/**
  * 加载csv文件处理
  * InkBamboo:测试通过
  */
object batchTable{
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit = {

    //初始化环境
    val batchenv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(batchenv)

    //加载csv文件
    val csvds:DataSet[(String,String,String,String,String)] = batchenv.readCsvFile("E:\\workspace\\big_data\\FlinkAllTest\\src\\main\\resources\\UserBehavior.csv")

    /****************************************************
      * 创建表 function One
      */

    tableEnv.registerDataSet("TestTwo",csvds,'id,'id2,'id3,'flag,'number)

    tableEnv.sqlQuery("select id,id2,id3,flag,number from TestTwo")

      /**此处的dataset需要指定typeinformation*/
      .toDataSet[(String,String,String,String,String)]

      .print()

    /**
      * 创建表 function Two
      */
    val tbl =  tableEnv.fromDataSet(csvds)

      /**为数据指定每列的名字。*/
      .as("id,id2,id3,flag,number")

    /**注册成表*/
    tableEnv.registerTable("testOne",tbl)

    val restbl =  tableEnv.sqlQuery("select * from testOne order by id limit 1000")

    //部分数据输出
    println("------------------------------------")
    tableEnv.toDataSet[Row](restbl).print()


    /***************************************
      * 结果数据写入外部文件系统 function One
      */
    val writeSink = new CsvTableSink(".\\res100")
    restbl.writeToSink(writeSink)

    /**
      * 结果数据写入外部文件系统 function Two
      */
    restbl.toDataSet[(String,String,String,String,String)].setParallelism(1).writeAsCsv("res.csv","\n",",",WriteMode.OVERWRITE)




    batchenv.execute("table_test")
  }
}


/**
  * InkBamboo :测试通过数据正常输出
  */
object TableAPITest {
  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val arr = Array(new DataPackage(1,"a",2),new DataPackage(2,"b",3),new DataPackage(3,"c",4))
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tblenv = TableEnvironment.getTableEnvironment(env)

    val source =  env.fromCollection(arr)
    val tbl1 = tblenv.fromDataSet(source).as("id,name,id2")   //as用逗号分割，重命名列名
    tblenv.registerTable("dataPackage",tbl1)

    val tblAPIResult = tblenv.scan("dataPackage").select("id,name,id2")
    tblenv.toDataSet[Row](tblAPIResult).print()

    //println("-----------------id--------"+tblAPIResult.toString())

    env.execute()

  }
  def flatfun(): Unit ={

  }
}

case class DataPackage(id:Int,name:String,id2:Int) extends Serializable
case class tableSchema(id:String,id2:String,id3:String,flag:String,number:String)