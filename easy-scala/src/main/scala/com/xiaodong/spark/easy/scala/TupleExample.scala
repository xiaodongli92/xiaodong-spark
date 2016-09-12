package com.xiaodong.spark.easy.scala

/**
  * Created by xiaodong on 2016/9/12.
  */
object TupleExample {
  def main(args: Array[String]): Unit = {
    val pair = (99, "basketball")
    println(pair._1)
    println(pair._2)
    val example = ("u","a","the",1,2,"me")
    println(example)
    println(example._5)
  }
}
