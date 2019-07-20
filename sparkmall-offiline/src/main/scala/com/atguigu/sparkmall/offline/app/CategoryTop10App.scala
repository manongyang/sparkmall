package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.offline.acc.MapAcc
import com.atguigu.sparkmall.offline.bean.CategoryCountInfo
import com.atguigu.sparkmall.offline.common.bean.UserVisitAction
import com.atguigu.sparkmall.offline.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10App {

  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {


    //累加器
    val acc = new MapAcc

    spark.sparkContext.register(acc, "userVisitActionAcc")


    //获取CategoryTop10

    userVisitActionRDD.foreach(x => {
      acc.add(x)
    })


    val sortedList = acc.value.toList.sortBy({
      case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
    }).take(10)

    val categoryTop10InfoList = sortedList.map(kv => {
      CategoryCountInfo(taskId, kv._1, kv._2._1, kv._2._2, kv._2._3)
    })

    //写入mysql中
    val sql = "insert into category_top10 values(?,?,?,?,?)"
    val args = categoryTop10InfoList.map(cci => {
      Array[Any](cci.taskId, cci.categoryId, cci.clickCount, cci.orderCount, cci.payCount)
    })

    JDBCUtil.executeUpdate("truncate category_top10 " , null)
    JDBCUtil.executeBatchUpdate(sql , args)

    categoryTop10InfoList

  }

}
