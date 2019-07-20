package com.atguigu.sparkmall.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.offline.app.{AreaProuctTop3, CategorySessionTop10, CategoryTop10App, PageConversionApp}
import com.atguigu.sparkmall.offline.bean.Condition
import com.atguigu.sparkmall.offline.common.bean.UserVisitAction
import com.atguigu.sparkmall.offline.common.util.ConfigurationUtil
import org.apache.spark.sql.SparkSession

object OfflineApp {

  def readUserVisitActionRDD(spark: SparkSession, condition: Condition) = {
    var sql =
      """
        |select
        |v.*
        |from user_visit_action v join user_info u
        |on v.user_id = u.user_id
        |where 1 = 1
      """.stripMargin

    if (isNotEmpty(condition.startDate)) {
      sql += s" and v.date >= '${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and v.date <= '${condition.endDate}'"
    }

    if (condition.startAge != 0) {
      sql += s" and u.age >= '${condition.startAge}'"
    }
    if (condition.endAge != 0) {
      sql += s" and u.age <= '${condition.endAge}'"
    }

    import spark.implicits._
    spark.sql("use sparkmall")
    val df = spark.sql(sql)

    df.as[UserVisitAction].rdd


  }

  def readConditions = {
    val config = ConfigurationUtil("conditions.properties")

    val conditionString = config.getString("condition.params.json")

    JSON.parseObject(conditionString, classOf[Condition])
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    //先获取sparkseesion
    val spark = SparkSession.builder()
      .appName("OfflineApp")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("hdfs://hadoop102:9000/sparkmall/userVisitActionRDD")
    val taskId = UUID.randomUUID().toString

    //根据条件过滤出需要的RDD，过滤条件定义在配置文件中

    val userVisitActionRDD = readUserVisitActionRDD(spark, readConditions)

    //rdd持久化

    userVisitActionRDD.cache()

    userVisitActionRDD.checkpoint()


    //需求1：
    val categroryTop10Result = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
//    categroryTop10Result.foreach(println)
    //需求2：
//        CategorySessionTop10.statCategoryTop10Seesion(spark,categroryTop10Result,userVisitActionRDD,taskId)

    //需求3:跳转率

//    PageConversionApp.statPageConversion(spark,userVisitActionRDD,"1,2,3,4,5,6,7",taskId)

    //需求4:
    AreaProuctTop3.statAreaProductTop3(spark,taskId)


  }

}
