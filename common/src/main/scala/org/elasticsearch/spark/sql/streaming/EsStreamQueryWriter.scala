package org.elasticsearch.spark.sql.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.rest.RestService
import org.elasticsearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.spark.rdd._
import org.elasticsearch.spark.sql.{DataFrameFieldExtractor, DataFrameValueWriter}

/**
  * Takes in iterator of
  */
private[sql] class EsStreamQueryWriter(serializedSettings: String,
                                       schema: StructType,
                                       commitProtocol: EsCommitProtocol)
  extends EsRDDWriter[InternalRow](serializedSettings) {

  override protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[DataFrameValueWriter]

  override protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]

  override protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[DataFrameFieldExtractor]

  private val encoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()

  override def write(taskContext: TaskContext, data: Iterator[InternalRow]): Unit = {
    // Keep clients from using this method, doesn't return task commit information.
    throw new EsHadoopIllegalArgumentException("Use run(taskContext, data) instead to retrieve the commit information")
  }

  /**
    * writer 是分到某个es的shard上，如果说index和time无关也就是说所有index不变，那么可以writer的实例化提出来。
    *
    * @param taskContext
    * @param data
    * @param indexTimeFieldName
    */
  def write(taskContext: TaskContext, data: Iterator[InternalRow], indexTimeFieldName: String): Unit = {
    // 指定取某个column作为index的时间后缀
    //    log.error("----- index field is : " + indexTimeFieldName)
    while (data.hasNext) {
      val res = processData(data)
      val (value, schemaStruct) = res.asInstanceOf[(GenericRowWithSchema, StructType)]
      val resource = value.getString(schemaStruct.fieldIndex(indexTimeFieldName)).substring(0, 10)
      settings.setResourceWrite(settings.getResourceWrite().replaceAll("\\*", resource))
      // log.error(" ------ write the resource is : " + settings.getResourceWrite)
      // create partition writer，但其实放在了while里面，也就是
      val writer = RestService.createWriter(settings, taskContext.partitionId.toLong, -1, log)
      taskContext.addTaskCompletionListener((TaskContext) => writer.close())
      if (runtimeMetadata) {
        writer.repository.addRuntimeFieldExtractor(metaExtractor)
      }
      writer.repository.writeToIndex(res)
    }
  }

  def run(taskContext: TaskContext, data: Iterator[InternalRow], indexTimeField: Option[String]): TaskCommit = {
//    settings.setResourceWrite(settings.getResourceWrite().replaceAll("\\*", resource))
    val taskInfo = TaskState(taskContext.partitionId(), settings.getResourceWrite)
    commitProtocol.initTask(taskInfo)
    try {
      indexTimeField match {
        case None =>
          //  super的 write，是每个partition写入相同的index，用同一个writer，也写入相同的es的shard
          super.write(taskContext, data)
        case Some(filedName) =>
          // 会遍历每一个item，生成resource，index，之后生成write，太慢了。
          write(taskContext, data, filedName)
      }
    } catch {
      case t: Throwable =>
        commitProtocol.abortTask(taskInfo)
        throw t
    }
    commitProtocol.commitTask(taskInfo)
  }

  override protected def processData(data: Iterator[InternalRow]): Any = {
    val row = encoder.fromRow(data.next())
    commitProtocol.recordSeen()
    (row, schema)
  }
}
