package com.xiaodong.spark.easy.scala

import org.apache.commons.lang3.StringUtils

import scala.io.Source

/**
  * Created by lixiaodong on 16/8/30.
  */
object Main {

  def main(args: Array[String]) : Unit = {
    readFile("/opt/kafka/LICENSE")
    println(add(1, 2))
    println(subStr("hello world"))
  }

  def add(one:Int, two:Int) : Int = {
    one + two
  }

  def subStr(str : String) : String = {
    StringUtils.substring(str, 0, 5);
  }

  def readFile(path : String) : Unit = {
    Source.fromFile(path,"UTF-8").foreach{lines =>
      print(lines)
    }
  }
}
