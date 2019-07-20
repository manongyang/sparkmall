package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.offline.common.util.MyKafkaUtil
import com.atguigu.sparkmall.realtime.app.BlackListApp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeAPP {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(2))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDSream(ssc, "ads_log")

    val adsInfoDStream = recordDstream.map {
      record =>
        val split = record.value.split(",")
        AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))
    }

    val filterAdsInfoDStream = BlackListApp.checkUserFromBlackList(adsInfoDStream,sc)

    BlackListApp.checkUserToBlackList(filterAdsInfoDStream)


    ssc.start()
    ssc.awaitTermination()



  }


}
