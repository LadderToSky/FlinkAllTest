package com.Inkbamboo.Flink.Table.batch

import java.util

import com.Inkbamboo.beans.TableSourceBean
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogTable}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row


/**
  * Author: inkbamboo
  * Date:   2019/8/12 20:36
  *
  * Think Twice, Code Once! 
  *
  * Desc:   各种数据源整理
  */
class TableSources {

  /**
    * 从数据集合中加载数据源
    * 从dataset中获取数据注册成表
    * 注册成表
    * @param tblenv
    * @param env
    */
  import org.apache.flink.api.scala._
  def sourceFromCollection(tblenv:BatchTableEnvironment,env:ExecutionEnvironment): Unit ={
    val arr:Array[TableSourceBean] = Array(new TableSourceBean(1,"a",2),new TableSourceBean(2,"b",3),new TableSourceBean(3,"c",4))
    val dateset= env.fromCollection(arr)
    val table = tblenv.fromDataSet(dateset).as("id,name,field")
    tblenv.registerTable("CollectionTable",table)

  }

  /**
    * 从csv文件中加载数据
    *
    * 从hdfs上的csv文件中读取文件
    */
  import org.apache.flink.table.api.scala._
    def sourceFromCSVFile(tblenv:BatchTableEnvironment,env:ExecutionEnvironment): Unit ={
      //加载csv文件
      //val csvds:DataSet[(String,String,String,String,String)] = env.readCsvFile("target/classes/UserBehavior.csv")
      //读取hdfs上的csv文件注册成表
      val csvds:DataSet[(String,String,String,String,Long)] = env.readCsvFile("hdfs:///zh/csvSinkTable/csvSinkTable.csv")
      tblenv.registerDataSet("CsvTable",csvds,'user_id,'pro_id,'pro_type_id,'act_type,'timestamp)

    }

  /**
    * table API中支持CSV,text,json等格式文件的读取
    *
    * 测试通过
    * @param tblenv
    * @param env
    */
  def sourceFromCSV2File(tblenv:BatchTableEnvironment,env:ExecutionEnvironment): Unit ={
    //使用CsvTableSource 读取csv格式的文件数据，并注册成表
    //flink集成了旧版本的csv文件，新版跟的需要导入flink-csv依赖。对应的json，parquet,avro格式的文件都需要依赖
    val source = CsvTableSource.builder()
      .path(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)
      .lineDelimiter("\n")
      .fieldDelimiter(",")
      .ignoreFirstLine()
      .field("user_id",Types.LONG)
      .field("pro_id",Types.LONG)
      .field("pro_type_id",Types.STRING)
      .field("act_type",Types.STRING)
      .field("timestamp",Types.STRING)
      .build()

    //注册成表
    tblenv.registerTableSource("csv2Tbl",source)


    val csvds:DataSet[(String,String,String,String,Long)] = env.readCsvFile("hdfs:///zh/csvSinkTable/csvSinkTable.csv")
    tblenv.registerDataSet("CsvTable",csvds,'user_id,'pro_id,'pro_type_id,'act_type,'timestamp)

  }

  /**
    *自定义注册外部数据源
    * 以文件系统为例，读取外部数据源
    */
  def sourceFromMysql(tblenv:BatchTableEnvironment,env:ExecutionEnvironment): Unit ={

    /**
      * 指定connector的类型 kafka/ fileSystem等 自定义
      */
    tblenv.connect(new FileSystem().path("/Users/inkbamboo/Documents/csv/"))
      //定义数据源获取的格式信息，现在看到的支持Csv和json
      //更多格式，需要自定义
      .withFormat(new Csv()
        .lineDelimiter("\n")
        .fieldDelimiter(",")
        .ignoreFirstLine()
        .field("user_id",Types.LONG)
        .field("pro_id",Types.LONG)
        .field("pro_type_id",Types.STRING)
        .field("act_type",Types.STRING)
        .field("timestamp",Types.STRING))
      //执行创建表的对应shcema信息
      //对于streamTable还可以在指定schema时，定义时间类型rowtime(event_time) or processtime 以及watermark等信息
      .withSchema(new Schema()
        .field("user_id",Types.LONG)
        .field("pro_id",Types.LONG)
        .field("pro_type_id",Types.STRING)
        .field("act_type",Types.STRING)
        .field("timestamp",Types.STRING))
      .registerTableSource("localFileSystemTbl")


    //测试读取数据
    tblenv.scan("localFileSystemTbl")
      .toDataSet[Row].print()

    tblenv.scan("localFileSystemTbl").printSchema()

    /**
      * root
      * |-- user_id: Long
      * |-- pro_id: Long
      * |-- pro_type_id: String
      * |-- act_type: String
      * |-- timestamp: String
      */
  }


}

/**
  * 数据源测试代码
  */
object TableSources extends App{

  private val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private val tblEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(environment)

  private val sources = new TableSources
  sources.sourceFromMysql(tblEnv,environment)
}
