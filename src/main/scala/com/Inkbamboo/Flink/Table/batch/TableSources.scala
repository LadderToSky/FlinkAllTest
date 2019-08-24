package com.Inkbamboo.Flink.Table.batch

import java.util

import com.Inkbamboo.beans.TableSourceBean
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.{ExternalCatalog, ExternalCatalogTable}
import org.apache.flink.table.descriptors.Kafka


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
    *自定义注册外部数据源
    * 以mysql数据源为例
    */
  def sourceFromMysql(tblenv:BatchTableEnvironment,env:ExecutionEnvironment): Unit ={

  }


}

/**
  * 自定义mysql数据源作为table的外部数据源
  *
  *
  */
/*class externalSourceMysql extends ExternalCatalog{
  override def getTable(tableName: String): ExternalCatalogTable ={
      return ExternalCatalogTable.builder((new Kafka)).asTableSink()
  }


  override def listTables(): util.List[String] = {

  }

  override def getSubCatalog(dbName: String): ExternalCatalog = {

  }

  override def listSubCatalogs(): util.List[String] = {

  }
}*/