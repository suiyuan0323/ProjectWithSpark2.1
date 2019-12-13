package org.apache.spark.kfParallelism

import com.sparkDataAnalysis.common.redis.{JedisClusterUtil, RedisOperator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/**
  *
  * @param redisAddress
  * @param redisPassword
  * @param redisKeyPrefix
  * @param redisKeyTimeOutMax
  * @param redisKeyTimeOutMin
  * @param redisReplicas
  * @param redisReplicasTimeoutMilliseconds
  * @param callOnOperator
  * @param callEndOperator
  */
case class OnlineAggUdf(redisAddress: String,
                        redisPassword: String,
                        redisKeyPrefix: String,
                        redisKeyTimeOutMax: Int,
                        redisKeyTimeOutMin: Int,
                        redisReplicas: Int,
                        redisReplicasTimeoutMilliseconds: Int,
                        callOnOperator: String,
                        callEndOperator: String) extends UserDefinedAggregateFunction with Logging {

  lazy val jedisCluster = JedisClusterUtil.getJedisCluster(redisAddress, redisPassword)

  override def inputSchema: StructType =
    new StructType()
      .add("modulePid", StringType)
      .add("sessionId", StringType)
      .add("eventName", StringType)

  override def bufferSchema: StructType =
    new StructType().add("onlineNum", StringType)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  // buffer: module-pid。最后是 num
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }


  /**
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) return
    if (buffer.getAs[String](0).isEmpty) buffer(0) = input.getString(0)
    val redisKey = s"""{$redisKeyPrefix${buffer.getAs[String](0)}-${input.getString(1)}"""
    val saveValue = Option(jedisCluster.get(redisKey)).getOrElse("")

    // if the key exists, value would by -1, 1, 0
    input.getString(2) match {
      case `callOnOperator` => {
        if (saveValue.isEmpty) {
          jedisCluster.setex(redisKey, redisKeyTimeOutMax, "1")
        } else if (saveValue.equals("-1")) {
          jedisCluster.setex(redisKey, redisKeyTimeOutMin, "0")
        }
      }
      case `callEndOperator` => {
        if (saveValue.isEmpty) {
          jedisCluster.setex(redisKey, redisKeyTimeOutMin, "-1")
        } else if (saveValue.equals("1")) {
          jedisCluster.setex(redisKey, redisKeyTimeOutMin, "0")
        }
      }
      case _ =>
    }
  }

  /**
    * 不存在merge，所有的value都在redis中，这里merge只是更新module+pid这个值
    * 这个值放在buffer中
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getAs[String](0).isEmpty && !buffer2.getAs[String](0).isEmpty) {
      buffer1(0) = buffer2.getAs[String](0)
    }
  }

  /**
    * 求redis中keys的值的sum
    *
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    import scala.collection.JavaConversions._
    var count = 0
    val redisKey = s"""{$redisKeyPrefix${buffer.getAs[String](0)}*"""
    try {
      // 有可能此时key正好都过期了，cluster.get会是null，直接处理，如果没取到redis的值，取默认值0
      count = new RedisOperator()
        .keys(redisKey, jedisCluster)
        .toList
        .map(jedisCluster.get(_).toInt.abs)
        .sum
    } catch {
      case ex: Exception =>
    } finally {
      jedisCluster.close()
    }
    count.toString
  }
}