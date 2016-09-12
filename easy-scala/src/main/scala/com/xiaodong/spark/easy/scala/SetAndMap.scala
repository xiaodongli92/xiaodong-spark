package com.xiaodong.spark.easy.scala

import scala.collection.{immutable, mutable}

/**
  * Created by xiaodong on 2016/9/12.
  */
object SetAndMap {
  def main(args: Array[String]): Unit = {
    var jetSet = Set("Beijing", "Shanghai")
    jetSet += "Nanjing"
    println(jetSet)
    println(jetSet.contains("Hangzhou"))

    val bookSet = mutable.Set("Spark", "Hadoop")
    bookSet += "Java"
    println(bookSet)

    val dateSet = immutable.Set("Monday", "Tuesday")
    println(dateSet + "Wednesday")

    val dateMap = mutable.Map[Int, String]()
    dateMap += (1 -> "Monday")
    dateMap += (2 -> "Tuesday")
    println(dateMap)
    dateMap.put(3, "Wednesday")
    println(dateMap)

    val romanNum = Map(1 -> "Ⅰ", 2 -> "Ⅱ")
    println(romanNum(2))
  }
}
