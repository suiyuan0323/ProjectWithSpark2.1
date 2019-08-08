package org.apache.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 按时间段做聚合
  *
  * @author xiaomei.wang
  * @date 2019/6/13 18:58
  * @version 1.0
  */
case class SpanSummary(spark: SparkSession) {

  import spark.implicits._

  val conf = spark.sparkContext.getConf

  /**
    * 将数据解析成需要的格式
    *
    * @param df
    * @return
    */
  def parseSpan(df: DataFrame) = {
    df.selectExpr("cast(value as String)")
      .where($"value".contains("47.97.102.70"))

  }
}
