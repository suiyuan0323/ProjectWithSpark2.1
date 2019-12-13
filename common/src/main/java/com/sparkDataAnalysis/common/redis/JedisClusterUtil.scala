package com.sparkDataAnalysis.common.redis

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

/**
  * @author xiaomei.wang
  * @date 2019/9/20 10:42
  * @version 1.0
  */
object JedisClusterUtil extends Serializable {

  def getJedisCluster(addStr: String, password: String) = {
    val addrs = addStr.split(",")
    val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
    var ipPortPair = new Array[String](2)
    for (i <- 0 until (addrs.length)) {
      ipPortPair = addrs(i).split(":")
      jedisClusterNodes.add(new HostAndPort(ipPortPair(0).trim, ipPortPair(1).trim.toInt))
    }
    val jedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(1024)
    jedisPoolConfig.setMaxIdle(10)
    jedisPoolConfig.setMaxWaitMillis(1000)
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPoolConfig.setTestOnReturn(true)
    new JedisCluster(jedisClusterNodes, 36000, 30000, 3, password, jedisPoolConfig)
  }
}
