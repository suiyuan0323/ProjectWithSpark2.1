package zipkin

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * offset保存到zookeeper上
  */
object KafkaOffset {
  /**
    * 从zk上拿offset
    */
  def readOffsets(topics: Seq[String], groupId: String, zkUtils: ZkUtils): Map[TopicPartition, Long] = {
    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)
    // /consumers/<groupId>/offsets/<topic>/
    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      //遍历每一个分区下的数据
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (offsetStatTuple != null) {
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), offsetStatTuple._1.toLong)
          }
        } catch {
          case e: Exception =>
            println("message: {} , topic: {}, partition: {},  node path: {}", e.getMessage, topics, topicPartitions, offsetPath)
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
        }
      })
    })
    topicPartOffsetMap.toMap
  }

  /**
    * 保存offset
    *
    * @param offsets
    * @param groupId
    * @param storeEndOffset true=保存结束offset， false=保存起始offset
    * @param zkUtils
    */
  def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean = true, zkUtils: ZkUtils): Unit = {
    offsets.foreach { or =>
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      println(or.topic.toString, or.partition.toString, offsetVal, offsetPath)
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition, offsetVal + "")
    }
  }
}
