package org.apache.spark

import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
  * @author xiaomei.wang
  * @date 2019/8/6 11:33
  * @version 1.0
  */
object RedisClient extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[4]")
      .config("redis.host", "10.24.1.54,10.24.1.54,10.24.1.55,10.24.1.55,10.24.1.57,10.24.1.57")
      .config("redis.port", "6379")
      .config("redis.auth", "AIspeech-324bb")
      .getOrCreate()

    val sc = spark.sparkContext
    val redisRdd = sc.parallelize(List(200))
    // sc.toRedisKV()
    val redisKeys: RDD[(String, String)] = sc.fromRedisKV("TEST_K_V")
    // TEST_K_V为redis中已存在的String类型的key   
    logError("总数" + redisKeys.count())
    redisKeys.foreach(map => println(map.toString()))
  }
}
