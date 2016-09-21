package com.xiaodong.spark.easy.scala

object FilterExample {

    def main(args: Array[String]) {
        val numbers = List(1, 2, 3, 4, 5)
        val numbersNew = numbers.filter(_ > 3)
        arrayPrint(numbers)
        println
        arrayPrint(numbersNew)
    }

    private def arrayPrint[T](args: List[Int]) = args.foreach(print)
}
