package com.atguigu.sparkmall.offline.common.util

import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {

  val dateSource: DataSource = initConnecton()


  /**
    * 初始化的连接
    *
    * @return
    */
  def initConnecton(): DataSource = {
    val properties = new Properties()
    val config = ConfigurationUtil("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getString("jdbc.url"))
    properties.setProperty("username", config.getString("jdbc.user"))
    properties.setProperty("password", config.getString("jdbc.password"))
    properties.setProperty("maxActive", config.getString("jdbc.maxActive"))
    DruidDataSourceFactory.createDataSource(properties)

  }


  /**
    * 执行单条语句
    *
    * @param sql
    * @param args
    */

  def executeUpdate(sql: String, args: Array[Any]) = {
    val connection = dateSource.getConnection
    connection.setAutoCommit(false)

    val ps = connection.prepareStatement(sql)

    if (args != null && args.length > 0) {
      (0 until args.length).foreach {
        i => ps.setObject(i + 1, args(i))
      }
    }

    ps.executeUpdate()
    connection.commit()

  }

  /**
    * 执行批处理
    *
    */

  def executeBatchUpdate(sql: String, argList: Iterable[Array[Any]]) = {

    val connection = dateSource.getConnection
    connection.setAutoCommit(false)

    val ps = connection.prepareStatement(sql)
    argList.foreach {
      case args: Array[Any] => {
        (0 until args.length).foreach {
          i => ps.setObject(i + 1, args(i))
        }
        ps.addBatch()
      }
    }
    ps.executeBatch()
    connection.commit()
  }


}
