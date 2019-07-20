object Test11 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession


    System.setProperty("HADOOP_USER_NAME" ,"atguigu")
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("MockOffline")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sql("create database sparkmall").show()
    spark.sql("show databases").show()


  }

}
