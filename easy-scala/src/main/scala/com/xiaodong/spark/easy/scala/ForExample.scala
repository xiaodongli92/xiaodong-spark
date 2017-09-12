package com.xiaodong.spark.easy.scala

object ForExample {

    def main(args: Array[String]) {
        val student = new Array[String](3)
        student(0) = "hello"
        student(1) = ","
        student(2) = "world"

        foreach1(student)
        println
        println("--------------------------")
        foreach2(student)
        println
        println("--------------------------")
        foreach3(student)
        println
        println("--------------------------")
        foreach4(student)
        println
        println("--------------------------")
        foreach4(foreach5(student))
        println
        println("--------------------------")
    }

    def foreach1(args : Array[String]):Unit = args.foreach(print)

    def foreach2(args : Array[String]):Unit = args.foreach((arg : String) => print(arg))

    def foreach3(args : Array[String]):Unit = {
        for (arg <- args) {
            print(arg)
        }
    }

    def foreach4(args : Array[String]):Unit = {
        val length = args.length - 1
        for (i <- 0 to length) {
            print(args(i))
        }
    }

    def foreach5(args: Array[String]):Array[String] = {
        for (arg <- args if arg == "hello") yield arg
    }
}
