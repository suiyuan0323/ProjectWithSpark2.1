package org.apache.spark.log

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.udf.TopNFun


/**
  * @author xiaomei.wang
  * @date 2019/8/28 14:57
  * @version 1.0
  */
case class BaserverTopN(spark: SparkSession) extends Logging {

  import spark.implicits._

  lazy val conf = spark.sparkContext.getConf

  def processLog(source: DataFrame) = {

    val topN = new TopNFun(5)

    val moduleStream = source.select(
      get_json_object($"value", "$.message").as("message"),
      get_json_object($"value", "$.module").as("module"),
      get_json_object($"value", "$.eventName").as("eventName"),
      get_json_object($"value", "$.businessType").as("businessType"),
      get_json_object($"value", "$.logTime").as("logTime"))
      .filter(
        s""" module = '${conf.get("spark.aispeech.data.module")}'
           | and eventName = '${conf.get("spark.aispeech.data.eventName")}'
           | and logTime is not null
           | and message is not null """.stripMargin)
    val productStream = moduleStream
      .withColumn("businessType", get_json_object($"message", "$.businessType"))
      .select(
        $"logTime".cast("timestamp"),
        when($"businessType".equalTo("BA"), get_json_object($"message", "$.message.input.product.id"))
          .otherwise(get_json_object($"message", "$.message.input.context.product.productId")).as("productId"))
      .filter("productId is not null")
      .withWatermark("logTime", conf.get("spark.aispeech.data.watermark.delay"))
      .groupBy(window($"logTime", "60 seconds", "60 seconds"))
      .agg(topN($"productId").as("topN"))

    //  .repartition(1) // 下一步合成一个
    //        .reduce()


    // s, sum(f) over (partition by i), row_number() over (order by f) from over10k where s = 'tom allen' or s = 'bob steinbeck';
    //  df.select(row_number().over(Window.partitionBy("value"))).collect())
    /* */
    //      .count()
    //      .sort($"count", $"col2".desc)
    // .limit(3).toDF("window", "productId", "count")
    productStream
  }
}
