package com.atguigu.sparkmall.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AreaClickUDAF extends UserDefinedAggregateFunction {


  override def inputSchema: StructType = {
    StructType(StructField("city", StringType) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("city_count_map", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val cityName = input.getString(0)
      var map = buffer.getMap[String, Long](0)
      buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)) {
      val map1 = buffer1.getMap[String, Long](0)

      val map2 = buffer2.getMap[String, Long](0)

      buffer1(0) = map1.foldLeft(map2) {
        case (map, (city, count)) => map + (city -> (map.getOrElse(city, 0L) + count))
      }

      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)


    }
  }

  override def evaluate(buffer: Row): Any = {
    val map = buffer.getAs[Map[String, Long]](0)
    val totalCount = buffer.getAs[Long](1)

    var cityRate = map.toList.sortBy(-_._2).take(2).map {
      case (cityName, count) => cityRemark(cityName, count.toDouble / totalCount)
    }
    cityRate :+= cityRemark("其他", cityRate.foldLeft(1d)(_ - _.rate))

    cityRate.mkString(",")

  }
}

case class cityRemark(cityName: String, rate: Double) {
  val f = new DecimalFormat(".00%")

  override def toString: String = s"$cityName:${f.format(rate)}"
}