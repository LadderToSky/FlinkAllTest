package com.Inkbamboo.Flink.Table.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
  * Author: inkbamboo
  * Date:   2019/8/12 22:01
  *
  * Think Twice, Code Once! 
  *
  * Desc:   
  */
object TableAPITest extends App {
  //数据源
  private val tableSources = new TableSources
  private val tableOperators = new TableOperators

  val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val tableEnvironment:BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

  //tableSources.sourceFromCollection(tableEnvironment,env)

  /**
    * 此处需要针对不同的数据源传入不同的数据类型，
    * 考虑使用泛型解决，还没找到有效手段
    * 暂时使用Row做统一处理(ps:原生Flink SQL中，都统一使用了一种叫 Row 的数据结构)
    */
  //tableSources.sourceFromCSVFile(tableEnvironment,env)
  //tableOperators.dataQuery(tableEnvironment,"CsvTable")

  //tableSources.sourceFromCSVFile(tableEnvironment,env)

  tableSources.sourceFromCSV2File(tableEnvironment,env)
  tableOperators.dataQuery(tableEnvironment,"csv2Tbl")
}
