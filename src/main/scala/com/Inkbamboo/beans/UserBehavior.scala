package com.Inkbamboo.beans

/**
*定义为pojo类
* pojo类定义：
* 个公共类，并且是独立的(不是一个非静态的内部类)
* 有一个公共的无参数构造函数
* 所有字段要么是公共的，要么有公共的getter和setter
*
* @param userId     用户ID
* @param itemId     商品ID
* @param categoryId 商品类目ID
* @param behavior   用户行为, 包括("pv", "buy", "cart", "fav")
* @param timestamp   行为发生的时间戳，单位秒
*/
class UserBehavior(
                     var userId: Long,
                     // 用户ID
                     var itemId: Long, // 商品ID
                     var categoryId: Int,// 商品类目ID
                     var behavior: String , // 用户行为, 包括("pv", "buy", "cart", "fav")
                     var timestamp: Long// 行为发生的时间戳，单位
                   ){
  //无参构造器
  def this(){
    this(0,0,0,null,0)
  }

  override def toString: String = ""+userId+"  "+itemId+" "+categoryId+" "+behavior+" "+timestamp+"->>>>"
}
