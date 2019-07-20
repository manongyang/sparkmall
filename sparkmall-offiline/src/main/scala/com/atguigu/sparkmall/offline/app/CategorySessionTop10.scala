package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import com.atguigu.sparkmall.offline.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object CategorySessionTop10 {

  def statCategoryTop10Seesion(spark: SparkSession, categoryTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {

    //1.获取top10 的品类id
    val categoryIdList: List[String] = categoryTop10.map(_.categoryId)

    //2.先过滤出来top10品类的id的用户行为数据
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => {
      if (uva.click_category_id != -1) {
        categoryIdList.contains(uva.click_category_id.toString)
      } else if (uva.order_category_ids != null) {
        val cids = uva.order_category_ids.split(",")
        categoryIdList.intersect(cids).nonEmpty
      } else if (uva.pay_category_ids != null) {
        val cids = uva.pay_category_ids.split(",")
        categoryIdList.intersect(cids).nonEmpty
      } else {
        false
      }

    })

    //统计每个品类的top10 session
    val categorySessionOne: RDD[((String, String), Int)] = filterUserVisitActionRDD.flatMap(uva => {
      if (uva.click_category_id != -1) {
        Array(((uva.click_category_id.toString, uva.session_id), 1))
      } else if (uva.order_category_ids != null) {
        val ids = uva.order_category_ids.split(",")
        categoryIdList.intersect(ids).map(id => {
          ((id, uva.session_id), 1)
        })
      } else {
        val ids = uva.pay_category_ids.split(",")
        categoryIdList.intersect(ids).map(id => {
          ((id, uva.session_id), 1)
        })
      }
    })

    //聚合
    val categorySessionCount: RDD[(String, (String, Int))] = categorySessionOne.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }

    val categeorySessionCountTop10: RDD[(String, List[(String, Int)])] = categorySessionCount.groupByKey().map {
      case (cid, sessionCountIt) => {
        (cid, sessionCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))
      }
    }


    val resultRDD = categeorySessionCountTop10.flatMap {
      case (cid, sessionCountList) => {
        sessionCountList.map {
          case (sid, sessionCount) => CategorySession(taskId, cid, sid, sessionCount)
        }
      }
    }

    resultRDD.foreach(println)


    //写入mysql

    import spark.implicits._
    val resultDF = resultRDD.toDF()
    val pros = new Properties()
    pros.setProperty("user","root")
    pros.setProperty("password" , "123")
    resultDF.write.mode("overwrite").jdbc("jdbc:mysql://hadoop102:3306/sparkmall","category_top10_session_count",pros)


  }
}
