package org.apache.spark.aispeech.udf

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions.lit

/**
  * @author xiaomei.wang
  * @date 2019/11/13 14:16
  * @version 1.0
  */
object PercentileApprox {

  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(col.expr, percentage.expr, accuracy.expr)
      .toAggregateExpression
    new Column(expr)
  }

  def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )
}
