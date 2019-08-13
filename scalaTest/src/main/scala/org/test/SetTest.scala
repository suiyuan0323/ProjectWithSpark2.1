package org.test

/**
  * @author xiaomei.wang
  * @date 2019/8/13 15:39
  * @version 1.0
  */
object SetTest {
  def main(args: Array[String]): Unit = {
    val set1 = Set("bz_ba_zipkin_spanspark-5","bz_ba_zipkin_spanspark-11",
      "bz_ba_zipkin_spanspark-6","bz_ba_zipkin_spanspark-3","bz_ba_zipkin_spanspark-4",
      "bz_ba_zipkin_spanspark-9","bz_ba_zipkin_spanspark-13","bz_ba_zipkin_spanspark-7",
      "bz_ba_zipkin_spanspark-12","bz_ba_zipkin_spanspark-14","bz_ba_zipkin_spanspark-0",
      "bz_ba_zipkin_spanspark-1","bz_ba_zipkin_spanspark-15",
      "bz_ba_zipkin_spanspark-10","bz_ba_zipkin_spanspark-8","bz_ba_zipkin_spanspark-2")
    val set2 = Set(Set("bz_ba_zipkin_spanSpark-5","bz_ba_zipkin_spanSpark-4","bz_ba_zipkin_spanSpark-3",
      "bz_ba_zipkin_spanSpark-2","bz_ba_zipkin_spanSpark-1","bz_ba_zipkin_spanSpark-0",
      "bz_ba_zipkin_spanSpark-13","bz_ba_zipkin_spanSpark-12","bz_ba_zipkin_spanSpark-11",
      "bz_ba_zipkin_spanSpark-10","bz_ba_zipkin_spanSpark-9","bz_ba_zipkin_spanSpark-8","bz_ba_zipkin_spanSpark-7",
      "bz_ba_zipkin_spanSpark-6","bz_ba_zipkin_spanSpark-15","bz_ba_zipkin_spanSpark-14"))
    println(set2 == set2)
    println(set1)
  }
}
