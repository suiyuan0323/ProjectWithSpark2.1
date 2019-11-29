package org.elasticsearch.spark.sql.sink

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.sql.streaming._

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.{Map => MutableMap}


class EsSinkProvider extends StreamSinkProvider with DataSourceRegister {
  private val logger: Log = LogFactory.getLog(classOf[EsSparkSqlStreamingSink])

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val mapConfig = MutableMap(parameters.toSeq: _*) += (INTERNAL_TRANSPORT_POOLING_KEY -> UUID.randomUUID().toString)
    val jobSettings = new SparkSettingsManager().load(sqlContext.sparkContext.getConf)
      .merge(streamParams(mapConfig.toMap, sqlContext.sparkSession).asJava)
    new EsStructedStreamingSink(sqlContext.sparkSession, jobSettings)
  }

  override def shortName(): String = "es-sink"

  private def streamParams(parameters: Map[String, String], sparkSession: SparkSession) = {
    // '.' seems to be problematic when specifying the options
    var params = parameters
      .map { case (k, v) => (k.replace('_', '.'), v) }
      .map {
        case (k, v) =>
          if (k.startsWith("es.")) (k, v)
          else if (k == "path") (ConfigurationOptions.ES_RESOURCE, v)
          else if (k == "queryname") (SparkSqlStreamingConfigs.ES_INTERNAL_QUERY_NAME, v)
          else if (k == "checkpointlocation") (SparkSqlStreamingConfigs.ES_INTERNAL_USER_CHECKPOINT_LOCATION, v)
          else ("es." + k, v)
      }

    params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_APP_NAME -> sparkSession.sparkContext.appName)
    params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_APP_ID -> sparkSession.sparkContext.applicationId)

    sparkSession.conf.getOption(SQLConf.CHECKPOINT_LOCATION.key).foreach {
      loc => params = params + (SparkSqlStreamingConfigs.ES_INTERNAL_SESSION_CHECKPOINT_LOCATION -> loc)
    }

    if (sparkSession.sparkContext.getConf.contains("spark.es.mapping.id"))
      params = params + (ConfigurationOptions.ES_MAPPING_ID -> sparkSession.sparkContext.getConf.get("spark.es.mapping.id"))

    params
  }
}