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


  /**
    * sql查询数据
    * @param tblEnv
    * @param tblNm
    */
  def dataQuery(tblEnv:BatchTableEnvironment, tblNm:String): Unit ={

    val resData = tblEnv.sqlQuery(
      s"""
        |select * from $tblNm
      """.stripMargin)



    import org.apache.flink.table.api.scala._
    val groubyData = resData
                      //查询指定字段
                      .select('user_id,'act_type,'timestamp)
                      //根据act_type 分组
                      .groupBy('act_type)
                      //将分组后的数据计数统计
                      .select('act_type,'timestamp.sum as 'sum2)

     //输出表的schema信息
     groubyData.printSchema()
    //转换为dataset数据并输出
    import org.apache.flink.api.scala._
    groubyData.toDataSet[Row].print()
  }
}
