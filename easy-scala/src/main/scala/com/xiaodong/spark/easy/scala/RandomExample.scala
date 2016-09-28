package com.xiaodong.spark.easy.scala

import scala.math.random

/**
  * Created by lixiaodong on 16/9/28.
  */
object RandomExample {
    def main(args: Array[String]): Unit = {
        println(piPositive(200000000))
        println(pi(200000000))
    }

    def pi(slices: Int): Double = {
        val n = math.min(slices, Int.MaxValue)
        var count:Double = 0
        for (i <- 0 until n) {
            val x = random * 2 - 1
            val y = random * 2 - 1
            if (x*x + y*y < 1)
                count += 1
        }
        4 * count/n
    }

    /**
      * 坐标x,y都是大于0的
      * @param slices
      * @return
      */
    def piPositive(slices: Int):Double = {
        val n = math.min(slices, Int.MaxValue)
        var count:Double = 0
        for (i <- 0 until n) {
            val x = random
            val y = random
            if (x*x + y*y < 1)
                count += 1
        }
        4 * count/n
    }
}
