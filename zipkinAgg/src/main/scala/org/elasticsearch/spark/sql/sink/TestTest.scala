package org.elasticsearch.spark.sql.sink

import org.apache.spark.aispeech.udf.SparkUserUDF

/**
  * @author xiaomei.wang
  * @date 2019/6/23 20:34
  * @version 1.0
  */
object TestTest {
  def main(args: Array[String]): Unit = {
    val testStr =
      """30df686f27e23a9b&10.244.43.225&1955,30df686f27e23a9b&10.244.43.225&1955,30df686f27e23a9b&10.244.43.225&1955,30df686f27e23a9b&10.244.43.225&1955,30df686f27e23a9b&10.244.43.225&1955,7fddfcc745706fac&10.244.43.225&2107,7fddfcc745706fac&10.244.43.225&2107,7fddfcc745706fac&10.244.43.225&2107,7fddfcc745706fac&10.244.43.225&2107,7fddfcc745706fac&10.244.43.225&2107,d0d1291f14421807&10.244.43.225&47,d0d1291f54421807&10.244.43.225&1747,d0d1291f54421807&10.244.43.225&1747,d0d1291f54421807&10.244.43.225&1747,d0d1291f54421807&10.244.43.225&1747"""
      .split(",").toList
    val udf = SparkUserUDF.distinctId(testStr, "&")
    val fromServiceDuration = SparkUserUDF.collectFromSericeWithDuration(testStr, "&")
    println(udf.size)
    udf.foreach(println(_))
    println(fromServiceDuration)
  }
}
