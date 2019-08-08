package org.apache.spark.aispeech.sink

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * @author xiaomei.wang
  * @date 2019/6/18 17:16
  * @version 1.0
  */
class KafkaSink(brokers: String, topic: String) extends ForeachWriter[Row] {

  var producer: KafkaProducer[String, String] = _
  val kafkaProps = new Properties
  kafkaProps.put("bootstrap.servers", brokers)
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  override def open(partitionId: Long, version: Long): Boolean = {
    producer = new KafkaProducer(kafkaProps)
    true
  }

  override def process(value: Row): Unit = {
    value match {
      case Row(traceId, id, serviceName, fromService, duration, path, method, logTime) => {}
        producer.send(new ProducerRecord[String, String](topic, (traceId + "," + id + "," + serviceName + "," + fromService + "," + duration + "," + path + "," + method + "," + logTime)))
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }
}
