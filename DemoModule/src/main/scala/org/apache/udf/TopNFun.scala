package org.apache.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author xiaomei.wang
  * @date 2019/8/29 18:22
  * @version 1.0
  */
case class TopNFun(topN: Int) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("inputStr", StringType, true) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("bufferMap", MapType(keyType = StringType, valueType = IntegerType), true) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = scala.collection.immutable.Map[String, Int]()
  }

  // 如果包含这个key则value+1,否则写入key,value=1
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val key = input.getAs[String](0)
    val imMap = buffer.getAs[Map[String, Int]](0)
    val bufferMap = scala.collection.mutable.Map[String, Int](imMap.toSeq: _*)
    val ret = if (bufferMap.contains(key)) {
      bufferMap.put(key, bufferMap(key) + 1)
      bufferMap
    } else {
      bufferMap.put(key, 1)
      bufferMap
    }
    buffer.update(0, ret)
  }

  // partition 之间 merge
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并两个map 相同的key的value累加
    val tempMap = (buffer1.getAs[Map[String, Int]](0) /: buffer2.getAs[Map[String, Int]](0)) ((map, kv) => {
      map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
    })
    buffer1.update(0, tempMap)
  }

  // output
  override def evaluate(buffer: Row): Any = {
    scala.collection.immutable.TreeMap(buffer.getAs[Map[String, Int]](0).map { case (k, v) => (v, k) }.toArray: _*)
      .takeRight(topN).mkString(",")
  }
}
