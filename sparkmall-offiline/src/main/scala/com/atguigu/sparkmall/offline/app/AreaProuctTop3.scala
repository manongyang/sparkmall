package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.offline.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.udf.AreaClickUDAF
import org.apache.spark.sql.SparkSession

object AreaProuctTop3 {

  def statAreaProductTop3(spark: SparkSession, taskId: String) = {
    spark.sql("use sparkmall")

    spark.udf.register("remark", new AreaClickUDAF)

    //1.用行为表和城市表,产品表做一张宽表,得到城市表的信息,产品名称,
    spark.sql(
      """
        |select
        | c.*,
        | p.product_name,
        | v.click_product_id
        |from user_visit_action v join city_info c join product_info p
        |on v.city_id = c.city_id and v.click_product_id = p.product_id
        |where v.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")


    //2.按照地区分组,统计每个产品的点击的数量
    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count(*) click_count ,
        |remark(city_name) remark
        |from t1
        |group by area , product_name
      """.stripMargin).createOrReplaceTempView("t2")


    //3.以area分区,按照点击数降序
    spark.sql(
      """
        |select
        |*,
        |rank() over(partition by area order by click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")


    //4.取前三
    val areaProductTop3Result = spark.sql(
      """
select
area,
product_name,
click_count,
remark
from t3
where rank <= 3
      """.stripMargin)


//    spark.sql("truncate table area_product_top3" )

    //写入数据库
    val pros = new Properties()
    pros.setProperty("user", "root")
    pros.setProperty("password", "123")
    areaProductTop3Result.write.mode("overwrite").jdbc("jdbc:mysql://hadoop102:3306/sparkmall?useUnicode=true&characterEncoding=utf8", "area_product_top3", pros)


  }

}
