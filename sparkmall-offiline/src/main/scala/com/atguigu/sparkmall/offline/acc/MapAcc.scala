package com.atguigu.sparkmall.offline.acc

import com.atguigu.sparkmall.offline.common.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2


class MapAcc extends AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] {

  var map = Map[String, (Long, Long, Long)]()


  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] = {
    val acc = new MapAcc

    acc.map = Map[String, (Long, Long, Long)]()

    acc

  }

  override def reset(): Unit = {
    map = Map[String, (Long, Long, Long)]()
  }

  override def add(v: UserVisitAction): Unit = {

    //点击行为
    if (v.click_category_id != -1) {
      val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(v.click_category_id.toString, (0L, 0L, 0L))

      map += v.click_category_id.toString -> (clickCount + 1, orderCount, payCount)

    } else if (v.order_category_ids != null) { //下单行为

      val ids = v.order_category_ids.split(",")
      ids.foreach(id => {

        var (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(id, (0L, 0L, 0L))

        map += id -> (clickCount, orderCount + 1, payCount)
      })
    } else if (v.pay_category_ids != null) { //付款行为

      //获取订单的所有品类id
      val pids = v.pay_category_ids.split(",")

      //遍历品类id数组，并计算
      pids.foreach(pid => {

        var (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(pid, (0L, 0L, 0L))

        map += pid -> (clickCount, orderCount, payCount + 1)
      })

    }

  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {

    val otherAcc = other.asInstanceOf[MapAcc]

    otherAcc.map.foreach {
      case (key, (clickCount1, orderCount1, payCount1)) =>
        val (clickCount2, orderCount2, payCount2) = this.map.getOrElse(key, (0L, 0L, 0L))
        this.map += key -> (clickCount1 + clickCount2, orderCount1 + orderCount2, payCount1 + payCount2)

    }


  }

  override def value: Map[String, (Long, Long, Long)] = map
}
