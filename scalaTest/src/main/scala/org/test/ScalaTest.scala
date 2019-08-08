package org.test

/**
  * @author xiaomei.wang
  * @date 2019/8/1 18:43
  * @version 1.0
  */
object ScalaTest {
  def main(args: Array[String]): Unit = {
    setAndOne
  }

  /**
    * 需求：spark中字段 isin  后面是不定长度参数，如果传入一个set，会认为只有一个值，类型是集合。所以需要处理下
    * : _*  是 unpacked 拆成单个的
    */
  def setAndOne = {
    val x: Seq[Seq[Int]] = Seq(Seq(1), Seq(2))

    def f(arg: Seq[Any]*): Int = {
      arg.length
    }

    val a = f(x) //1 as x is taken as single arg
    val b = f(x: _*) // 2 as x is "unpacked" as a Seq[Any]*
    println(a)
    println(b)
  }

}
