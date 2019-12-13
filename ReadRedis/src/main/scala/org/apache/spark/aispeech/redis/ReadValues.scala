package org.apache.spark.aispeech.redis

import com.sparkDataAnalysis.common.redis.{JedisClusterUtil, RedisOperator}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.JedisCluster

/**
  * @author xiaomei.wang
  * @date 2019/11/5 15:18
  * @version 1.0
  */
object ReadValues extends Logging {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder()
      .appName(conf.get("spark.aispeech.queryName"))
      .getOrCreate()

    val redisAddress = args(0)
    val redisPassword = args(1)
    val redisKey = args(2)
    val jedisCluster = JedisClusterUtil.getJedisCluster(redisAddress, redisPassword)

    logWarning("--- start")

    conf.get("spark.aispeech.redis.options", "").trim.toLowerCase().split(",").foreach {
      case "search" => spark.sparkContext.parallelize(getKeysMessage(jedisCluster, redisKey), 1)
        .collect()
        .foreach {
          case (key, value, times, slot, port) =>
            logWarning(s"------search:key->$key,value->$value, time->$times, slot->$slot, port->$port ")
        }
      case "delete" => spark.sparkContext.parallelize(deleteKeys(jedisCluster, redisKey), 1)
        .collect()
        .foreach {
          case (key, nums) => logWarning("------delete " + key + "->" + nums + "个")
        }
      case _ => logError("--- no option ")
    }

    logWarning("--- end")
    jedisCluster.close()
    spark.close()
  }

  def getKeysMessage(jedisCluster: JedisCluster, redisKey: String) = {
    logWarning("--- search")
    import scala.collection.JavaConversions._
    import redis.clients.jedis.util.JedisClusterCRC16
    new RedisOperator()
      .keys(redisKey, jedisCluster)
      .toList
      .map(
        key =>
          Tuple5(
            key,
            jedisCluster.get(key),
            jedisCluster.ttl(key),
            JedisClusterCRC16.getSlot(key),
            jedisCluster.getConnectionFromSlot(JedisClusterCRC16.getSlot(key)).getClient().getPort())
      )
  }

  def deleteKeys(jedisCluster: JedisCluster, redisKey: String) = {
    logWarning("--- delete")
    import scala.collection.JavaConversions._
    new RedisOperator()
      .keys(redisKey, jedisCluster)
      .toList
      .map(key => Tuple2(key, jedisCluster.del(key)))
  }
}
