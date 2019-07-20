package com.atguigu.sparkmall.realtime

import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts: Long, area: String, city: String, userId: String, adsId: String) {
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

  override def toString: String = s"${dayString}:${area}:${city}:${adsId}"

}
