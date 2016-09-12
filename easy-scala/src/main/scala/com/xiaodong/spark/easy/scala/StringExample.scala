package com.xiaodong.spark.easy.scala

/**
  * Created by xiaodong on 2016/9/12.
  */
object StringExample {
  def main(args: Array[String]): Unit = {
    val dateArray = Array("Monday", "Tuesday", "Wednesday")
    print(formatString(dateArray))
  }

  def formatString(args: Array[String]) = args.mkString("\n")
}
