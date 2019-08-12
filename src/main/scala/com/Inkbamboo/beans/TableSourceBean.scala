package com.Inkbamboo.beans

/**
  * Author: inkbamboo
  * Date:   2019/8/12 21:55
  *
  * Think Twice, Code Once! 
  *
  * Desc:   TableApi测试数据源类
  * 定义为pojo类型
  */
class TableSourceBean(id:Int,name:String,field:Int) {

  def this(){
    this(0,null,0)
  }

  override def toString: String = {
    ""+"id:"+id+"     name:"+name+"    field:"+field
  }
}

