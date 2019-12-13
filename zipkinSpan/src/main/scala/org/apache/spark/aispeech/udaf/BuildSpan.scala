package org.apache.spark.aispeech.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

case class BuildSpan(kindServer: String = "SERVER",
                     kindClient: String = "CLIENT") extends UserDefinedAggregateFunction {

  val SPAN = "span"

  override def inputSchema: org.apache.spark.sql.types.StructType =
    new StructType()
      .add(StructField("kind", StringType))
      .add(StructField("logTime", StringType))
      .add(StructField("duration", LongType))
      .add(StructField("serviceName", StringType))
      .add(StructField("fromService", StringType))
      .add(StructField("method", StringType))
      .add(StructField("path", StringType))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType =
    new StructType()
      .add(StructField("kind", StringType))
      .add(StructField("logTime", StringType))
      .add(StructField("duration", LongType))
      .add(StructField("serviceName", StringType))
      .add(StructField("fromService", StringType))
      .add(StructField("method", StringType))
      .add(StructField("path", StringType))

  // This is the output type of your aggregatation function.
  // logTime, duration, serviceName, fromService, method, path
  override def dataType: DataType = DataTypes.StringType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    buffer(1) = ""
    buffer(2) = 0L
    buffer(3) = ""
    buffer(4) = ""
    buffer(5) = ""
    buffer(6) = ""
  }

  def getServiceMessage(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    // kind
    if (buffer1.getString(0).equals(kindClient)) buffer1.update(0, SPAN)
    else if (buffer1.getString(0).equals("")) buffer1.update(0, kindServer)
    // logTime
    if (buffer1.getString(1).equals("")) buffer1.update(1, buffer2.getString(1))
    // serviceName
    buffer1.update(3, buffer2.getString(3))
    // fromService
    if (buffer1.getString(4).equals("")) buffer1.update(4, buffer2.getString(4))
    buffer1.update(5, buffer2.getString(5))
    buffer1.update(6, buffer2.getString(6))
    buffer1
  }

  def getClientMessage(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    // kind
    if (buffer1.getString(0).equals(kindServer)) buffer1.update(0, SPAN)
    else if (buffer1.getString(0).equals("")) buffer1.update(0, kindClient)
    // logTime
    buffer1.update(1, buffer2.getString(1))
    // fromService
    buffer1.update(4, buffer2.getString(3))
  }


  /**
    * @param buffer kind(client, server, span), logTime, duration, serviceName, fromService, method, path
    * @param input  kind, logTime, duration, serviceName, fromService, method, path
    */
  override def update(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer2.isNullAt(0)) return
    // duration
    buffer1.update(2, Math.max(buffer1.getLong(2), buffer2.getLong(2)))
    buffer2.getString(0) match {
      case kind if kind.equals(kindServer) => {
//        println("------update server: buffer1， " + buffer1.mkString(",") + "buffer2, " + buffer2.mkString(",") )
        // kind
        if (buffer1.getString(0).equals(kindClient)) buffer1.update(0, SPAN)
        else if (buffer1.getString(0).equals("")) buffer1.update(0, kindServer)
        // logTime
        if (buffer1.getString(1).equals("")) buffer1.update(1, buffer2.getString(1))
        // serviceName
        buffer1.update(3, buffer2.getString(3))
        // fromService
        if (buffer1.getString(4).equals("")) buffer1.update(4, buffer2.getString(4))
        buffer1.update(5, buffer2.getString(5))
        buffer1.update(6, buffer2.getString(6))
      }
      case kind if kind.equals(kindClient) => {
//        println("------update client: buffer1， " + buffer1.mkString(",") + "buffer2, " + buffer2.mkString(",") )
        // kind
        if (buffer1.getString(0).equals(kindServer)) buffer1.update(0, SPAN)
        else if (buffer1.getString(0).equals("")) buffer1.update(0, kindClient)
        // logTime
        buffer1.update(1, buffer2.getString(1))
        // fromService
        buffer1.update(4, buffer2.getString(4))
      }
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // duration
    buffer1(2) = Math.max(buffer1.getLong(2), buffer2.getLong(2))
    buffer2.getString(0) match {
      case kind if kind.equals(kindServer) => {
//        println("------merge server: buffer1， " + buffer1.mkString(",") + "buffer2, " + buffer2.mkString(",") )
        // kind
        if (buffer1.getString(0).equals(kindClient)) buffer1.update(0, SPAN)
        else if (buffer1.getString(0).equals("")) buffer1.update(0, kindServer)
        // logTime
        if (buffer1.getString(1).equals("")) buffer1.update(1, buffer2.getString(1))
        // serviceName
        buffer1.update(3, buffer2.getString(3))
        // fromService
        if (buffer1.getString(4).equals("")) buffer1.update(4, buffer2.getString(4))
        buffer1.update(5, buffer2.getString(5))
        buffer1.update(6, buffer2.getString(6))
      }
      case kind if kind.equals(kindClient) => {
//        println("------merge client: buffer1， " + buffer1.mkString(",") + "buffer2, " + buffer2.mkString(",") )
        // kind
        if (buffer1.getString(0).equals(kindServer)) buffer1.update(0, SPAN)
        else if (buffer1.getString(0).equals("")) buffer1.update(0, kindClient)
        // logTime
        buffer1.update(1, buffer2.getString(1))
        // fromService
        buffer1.update(4, buffer2.getString(4))
      }
      case SPAN => {
        if (!buffer2.getString(0).equals("")) buffer1.update(0, buffer2.getString(0))
        if (!buffer2.getString(1).equals("")) buffer1.update(1, buffer2.getString(1))
        if (!buffer2.getString(3).equals("")) buffer1.update(3, buffer2.getString(3))
        if (!buffer2.getString(4).equals("")) buffer1.update(4, buffer2.getString(4))
        if (!buffer2.getString(5).equals("")) buffer1.update(5, buffer2.getString(5))
        if (!buffer2.getString(6).equals("")) buffer1.update(6, buffer2.getString(6))
      }
    }
  }

  // kind, logTime, duration, serviceName, fromService, method, path
  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    s"""${buffer.getString(1)},${buffer.getLong(2).toString},${buffer.getString(3)},${buffer.getString(4)},${buffer.getString(5)},${buffer.getString(6)}"""
  }
}
