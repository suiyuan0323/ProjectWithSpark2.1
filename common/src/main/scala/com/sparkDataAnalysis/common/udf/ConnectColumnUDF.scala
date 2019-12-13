package com.sparkDataAnalysis.common.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 将groupBy后的 column, 拼接成字符串
  * @author xiaomei.wang
  * @date 2019/11/5 11:45
  * @version 1.0
  */
case class ConnectColumnUDF(connectChar: String) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("buffer", StringType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = ""

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0) || input.getAs[String](0).isEmpty) return
    buffer(0) = buffer.getAs[String](0) + connectChar + input.getAs[String](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = (buffer1.getAs[String](0) + connectChar + buffer2.getAs[String](0))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
      .split(connectChar).filter(_.length > 0) // 去掉空的
      .mkString(connectChar)
  }
}
