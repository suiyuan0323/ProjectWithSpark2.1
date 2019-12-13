package org.test

/**
  * @author xiaomei.wang
  * @date 2019/9/9 11:18
  * @version 1.0
  */
object caseMatchTest {
  def main(args: Array[String]): Unit = {
    val addOperation = "charge"
    val subOperation = "call-end"

    val testString = "charge"

    testString match {
      case `addOperation` => println("+")
      case `subOperation` => println("-")
      case _ => println("nothing match")
    }
  }
}
