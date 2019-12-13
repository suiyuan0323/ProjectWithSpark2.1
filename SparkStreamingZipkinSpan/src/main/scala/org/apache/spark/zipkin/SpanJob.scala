package org.apache.spark.zipkin

import java.util.Properties
import kafka.utils.ZkUtils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import zipkin.{KafkaOffset, MessageToSpan}

/**
  * 消费kafka消息，合并其中的spanid相同的log为一个span，将合并好的span发给kakfa
  *
  * @author xiaomei.wang
  * @date 2019/6/13 16:59
  * @version 1.0
  */
object SpanJob extends Logging {

  val KIND_SERVER = "SERVER"
  val KIND_CLIENT = "CLIENT"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val checkpointDirectory = conf.get("spark.aispeech.checkpoint")
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => functionToCreateContext(checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(checkpointDirectory: String): StreamingContext = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(conf.get("spark.aispeech.streaming.duration.Seconds").toInt))

    execute2(ssc)
    //    execute(ssc)

    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def execute(ssc: StreamingContext) = {
    val (durationSeconds, excludeServices, kafkaBrokers, consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut) = (
      ssc.sparkContext.getConf.get("spark.aispeech.streaming.duration.Seconds").toInt,
      ssc.sparkContext.getConf.get("sspark.aispeech.data.exclude.services"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.brokers"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.consumer.groupId"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.consumer.topics"),
      ssc.sparkContext.getConf.get("spark.aispeech.zk.hosts"),
      ssc.sparkContext.getConf.get("spark.aispeech.zk.session.timeout").toInt,
      ssc.sparkContext.getConf.get("spark.aispeech.zk.connection.timeout").toInt
    )
    readKafkaToSpan(ssc, kafkaBrokers, consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut)
      .foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map { case item => MessageToSpan.parseMessage(item.value()) }
          .filter { case spanMessage => MessageToSpan.filterServiceName(spanMessage, excludeServices.split(",").toList) }
          .reduceByKey(MessageToSpan.combineLogToSpan(_, _))
          .map { case (_, values) =>
            var res = values.-("kind")
            res = res.updated("fromService", (if (values.get("remoteName").get.equals("")) "unknown" else values.get("remoteName").get))
            res = res.-("remoteName")
            compact(render(res)) // 转成json
          }.foreachPartition { partitionRdd =>
          val kafkaProdcer = new KafkaProducer[String, String](getProducerProp(kafkaBrokers))
          partitionRdd foreach { message => kafkaProdcer.send(new ProducerRecord(consumerTopics, message)) }
        }
        KafkaOffset.persistOffsets(offsetRanges, consumerGroupId, true, getZkUtils(consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut))
      }
  }

  /**
    * 读取kafka消息，合成span，发给kafka
    *
    * @param ssc
    */
  def execute2(ssc: StreamingContext) = {
    val (durationSeconds, excludeServices, kafkaBrokers, consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut) = (
      ssc.sparkContext.getConf.get("spark.aispeech.streaming.duration.Seconds").toInt,
      ssc.sparkContext.getConf.get("sspark.aispeech.data.exclude.services"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.brokers"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.consumer.groupId"),
      ssc.sparkContext.getConf.get("spark.aispeech.kafka.consumer.topics"),
      ssc.sparkContext.getConf.get("spark.aispeech.zk.hosts"),
      ssc.sparkContext.getConf.get("spark.aispeech.zk.session.timeout").toInt,
      ssc.sparkContext.getConf.get("spark.aispeech.zk.connection.timeout").toInt
    )

    var offsetRanges = Array[OffsetRange]()

    val logStream = readKafkaToSpan(ssc, kafkaBrokers, consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut)
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map { case item => MessageToSpan.parseMessage(item.value()) }
      .filter { case spanMessage => MessageToSpan.filterServiceName(spanMessage, excludeServices.split(",").toList) }

    /* logStream.reduceByKeyAndWindow(MessageToSpan.combineLogToSpan(_, _), Seconds(durationSeconds * 2), Seconds(durationSeconds))
      .map { case (_, values) =>
        var res = values.-("kind")
        res = res.updated("fromService", (if (values.get("remoteName").get.equals("")) "unknown" else values.get("remoteName").get))
        res = res.-("remoteName")
        compact(render(res)) // 转成json
      }*/
    val serviceStream = logStream.window(Seconds(durationSeconds), Seconds(durationSeconds))
    val clientStream = logStream.window(Seconds(durationSeconds * 2), Seconds(durationSeconds))
    val joinedStream = serviceStream.join(clientStream)
    /*.foreachRDD { rdd =>
      rdd.foreachPartition { partitionRdd =>
        val kafkaProdcer = new KafkaProducer[String, String](getProducerProp(kafkaBrokers))
        partitionRdd.foreach { message => kafkaProdcer.send(new ProducerRecord(consumerTopics, message)) }
      }
      KafkaOffset.persistOffsets(offsetRanges, consumerGroupId, true, getZkUtils(consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut))
    }*/
  }


  /**
    * 消费kafka消息
    *
    * @param ssc
    * @return
    */
  def readKafkaToSpan(ssc: StreamingContext, kafkaBrokers: String, consumerGroupId: String, consumerTopics: String,
                      zkUtils: String, zkSessionTimeOut: Int, zkConnectionTimeOut: Int) = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        consumerTopics.split(",").toList,
        kafkaParams,
        KafkaOffset.readOffsets(consumerTopics.split(",").toSeq, consumerGroupId, getZkUtils(consumerGroupId, consumerTopics, zkUtils, zkSessionTimeOut, zkConnectionTimeOut))))
  }

  private def getZkUtils(consumerGroupId: String, consumerTopics: String, zkUtils: String, zkSessionTimeOut: Int, zkConnectionTimeOut: Int) = {
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(
      zkUtils + "/consumers/" + consumerGroupId + "/offsets/" + consumerTopics.split(",").toSeq,
      zkSessionTimeOut, zkConnectionTimeOut)
    new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
  }

  private def getProducerProp(kafkaBrokers: String) = {
    val kafkaProp = new Properties()
    kafkaProp.put("bootstrap.servers", kafkaBrokers)
    kafkaProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProp.put("request.required.acks", "1")
    kafkaProp.put("producer.type", "async")
    kafkaProp
  }
}
