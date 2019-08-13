package org.apache.spark.sink

import java.text.SimpleDateFormat
import java.util.Calendar
import org.aispeech.RedisClusterUtil
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.JedisCluster

/**
  * @author xiaomei.wang
  * @date 2019/8/9 12:11
  * @version 1.0
  */
case class RedisWriter(conf: SparkConf) extends ForeachWriter[Row] with Logging {

  var jedis: JedisCluster = _

  override def open(partitionId: Long, version: Long): Boolean = {
    jedis = RedisClusterUtil.getCluster(conf.get("spark.aispeech.write.redis.nodes"), conf.get("spark.aispeech.write.redis.password"))
    true
  }

  /**
    * 每秒更一次redis，凌晨的时候new 一个redis的key，将上一天的value，保存到mysql中。
    *
    * @param value
    */
  override def process(value: Row): Unit = {
    val prefixKey = conf.get("spark.aispeech.write.redis.key")
    val cal: Calendar = Calendar.getInstance()
    val dayStr = new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
    val redisCurKey = prefixKey + dayStr
    logError("------row" + value.mkString(","))
    try {
      //      jedis.setex(redisCurKey, 86700, String.valueOf("10"))
      value match {
        case Row(count) => {
          logError("---- count is : " + count)
          if (!jedis.exists(redisCurKey)) {
            // 程序重启、第二天
            // "EX" is seconds, 86700 seconds = 24 hours * 60 * 60 + 5 minutes * 60
            jedis.setex(redisCurKey, 86700, String.valueOf(count))
          } else {
            // update
            logError("----old redis value : " + count)
            jedis.setrange(redisCurKey, 0, String.valueOf(jedis.get(redisCurKey).toLong + count.asInstanceOf[Int]))
          }
        }
      }
    } catch {
      case ex: Exception => logError("____error:" + ex.getMessage + "  --- row: " + value.mkString(","))
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      jedis.close()
    } catch {
      case e: Exception => {
        logError("---------redis: " + e.getMessage)
      }
    }
  }
}
