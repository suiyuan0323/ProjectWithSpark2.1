package org.elasticsearch.spark

import org.apache.commons.logging.LogFactory
import org.apache.spark.Partitioner
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.RestRepository


class ESShardPartitioner(settings: String) extends Partitioner {
  protected val log = LogFactory.getLog(this.getClass())

  protected var _numPartitions = -1

  override def numPartitions: Int = {
    val newSettings = new PropertiesSettings().load(settings)
    //        newSettings.setResourceRead("web/blog") // ******************** !!! modify it !!! ********************
    //        newSettings.setResourceWrite("web/blog") // ******************** !!! modify it !!! ********************
    val repository = new RestRepository(newSettings)
    val targetShards = repository.getWriteTargetPrimaryShards(newSettings.getNodesClientOnly())
    repository.close()
    _numPartitions = targetShards.size()
    _numPartitions
  }

  override def getPartition(key: Any): Int = {
    val shardId = ShardAlg.shard(key.toString(), _numPartitions)
    shardId
  }
}


object ShardAlg {
  def shard(id: String, shardNum: Int): Int = {
    mod(Murmur3HashFunction.hash(id), shardNum)
  }

  def mod(v: Int, m: Int): Int = {
    val r = v % m
    if (r < 0) r + m
    else r
  }
}

/**
  * es java源码，内部的hash算法
  * org.elasticsearch.cluster.routing
  */
object Murmur3HashFunction {
  def hash(routing: String): Int = {
    val bytesToHash = Array.ofDim[Byte](routing.length * 2)

    for (i <- 0 until bytesToHash.length) {
      val c = routing.charAt(i)
      val b1 = c.toByte
      val b2 = (c >>> 8).toByte

      assert(((b1 & 0xFF) | ((b2 & 0xFF) << 8)) == c)

      bytesToHash(i * 2) = b1
      bytesToHash(i * 2 + 1) = b2
    }

    hash(bytesToHash, 0, bytesToHash.length)
  }

  def hash(bytes: Array[Byte], offset: Int, length: Int): Int = {
    murmurhash3_x86_32(bytes, offset, length, 0)
  }

  /**
    * scala实现的
    * 参考 org.apache.lucene.util.StringHelper
    *
    * @param data
    * @param offset
    * @param len
    * @param seed
    * @return
    */
  def murmurhash3_x86_32(data: Array[Byte],
                         offset: Int,
                         len: Int,
                         seed: Int): Int = {
    val c1 = 0xcc9e2d51
    val c2 = 0x1b873593
    var h1 = seed
    val roundedEnd = offset + (len & 0xfffffffc)
    var i = offset
    while (i < roundedEnd) {
      var k1 = (data(i) & 0xff) | ((data(i + 1) & 0xff) << 8) | ((data(i + 2) & 0xff) << 16) |
        (data(i + 3) << 24)
      k1 *= c1
      k1 = (k1 << 15) | (k1 >>> 17)
      k1 *= c2
      h1 ^= k1
      h1 = (h1 << 13) | (h1 >>> 19)
      h1 = h1 * 5 + 0xe6546b64
      i += 4
    }
    var k1 = 0
    len & 0x03 match {
      case 3 => k1 = (data(roundedEnd + 2) & 0xff) << 16
      case 2 => k1 |= (data(roundedEnd + 1) & 0xff) << 8
      case 1 =>
        k1 |= (data(roundedEnd) & 0xff)
        k1 *= c1
        k1 = (k1 << 15) | (k1 >>> 17)
        k1 *= c2
        h1 ^= k1
      case _ => //break
    }
    h1 ^= len
    h1 ^= h1 >>> 16
    h1 *= 0x85ebca6b
    h1 ^= h1 >>> 13
    h1 *= 0xc2b2ae35
    h1 ^= h1 >>> 16
    h1
  }
}