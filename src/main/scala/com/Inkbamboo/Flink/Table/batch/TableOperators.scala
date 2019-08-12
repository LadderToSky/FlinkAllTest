package com.Inkbamboo.Flink.Table.batch

import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * Author: inkbamboo
  * Date:   2019/8/12 21:47
  *
  * Think Twice, Code Once! 
  *
  * Desc:   tableAPI涉及到的所有操作算子
  */
class TableOperators {

  def dataQuery(tblEnv:BatchTableEnvironment, tblNm:String): Unit ={

    val resData = tblEnv.sqlQuery(
      s"""
        |select * from $tblNm
      """.stripMargin)

    import org.apache.flink.api.scala._
    tblEnv.toDataSet[Row](resData)
      .print()
  }
}
