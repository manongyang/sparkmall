package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.offline.common.util.RedisUtil
import com.atguigu.sparkmall.realtime.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream


object BlackListApp {

  val countKey = "user:day:adsClick"
  val blackListKey = "blackllist"



  //将用户写入黑名单中
  def checkUserToBlackList(adsInfoDStream : DStream[AdsInfo])={

    adsInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(adsInfoIt => {
        val jedis = RedisUtil.getJedisClient
        adsInfoIt.foreach( adsInfo =>{
          val countFiled = s"${adsInfo.userId}:${adsInfo.dayString}:${adsInfo.adsId}"

          val dayUserAdsCount = jedis.hincrBy(countKey,countFiled,1)
          if(dayUserAdsCount >= 100 ){
            jedis.sadd(blackListKey ,adsInfo.userId)
          }
        })
        jedis.close()
      })
    })
  }

  //过滤黑名单的用户
  def checkUserFromBlackList(adsClikInfoDStream :DStream[AdsInfo] , sc :SparkContext) ={

    adsClikInfoDStream.transform(rdd => {
      val jedis = RedisUtil.getJedisClient

      //从redi读出黑名单
      val blackList = jedis.smembers(blackListKey)

      val bcBlackList = sc.broadcast(blackList)

      jedis.close()

      rdd.filter(adsInfo =>{
        !bcBlackList.value.contains(adsInfo.userId)
      })

    })
  }

}
