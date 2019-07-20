package com.atguigu.sparkmall.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.offline.common.bean.UserVisitAction
import com.atguigu.sparkmall.offline.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {

  def statPageConversion(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPageFlow: String, taskId: String) = {


    //1.定基准，得到的数组 1->2 , 2->3 , 3->4 , 4->5
    val pageFlows = targetPageFlow.split(",")

    val prePageFlow = pageFlows.slice(0, pageFlows.length - 1)

    val postPageFlow = pageFlows.slice(1, pageFlows.length)

    val targetJumpPages = prePageFlow.zip(postPageFlow).map {
      case (x, y) => x + "->" + y
    }

    //2.过滤  并计算页面的总数
    val targetPageCount = userVisitActionRDD.filter(uva => pageFlows.contains(uva.page_id.toString))
      .map(uva => (uva.page_id, 1))
      .countByKey()

    //3.按照session分组,并统计每组内的userVisitAction进行排序

    //zhu
    val pageJumpRDD = userVisitActionRDD.groupBy(_.session_id).flatMap {
      case (sid, actions) => {
        val visitActions = actions.toList.sortBy(_.action_time)
        val pre = visitActions.slice(0, visitActions.length - 1)
        val post = visitActions.slice(1, visitActions.length)

        //
        pre.zip(post).map(t => t._1.page_id + "->" + t._2.page_id).filter(targetJumpPages.contains(_))

      }
    }

    //4.统计跳转次数
    val pageJumpCount = pageJumpRDD.map((_, 1)).reduceByKey(_ + _).collect

    //5.计算跳转率
    val formatter = new DecimalFormat(".00%")

    val conversionRate = pageJumpCount.map {
      case (flow, count) => {
        val key = flow.split("->")(0)

        val visitPageCount = targetPageCount.getOrElse(key.toLong, -1L)

        val rate = formatter.format(count.toDouble / visitPageCount)

        (flow ,rate)
      }
    }

    //6.传入数据库

    val conversionRateResult = conversionRate.map {
      case (flow, rate) => Array[Any](taskId, flow, rate)
    }

    JDBCUtil.executeUpdate("truncate  page_conversion_rate" , null)

    JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?,?,?)" , conversionRateResult)
  }

}
