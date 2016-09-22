package com.xiaodong.spark.easy.scala

object EnumExample {
    def main(args: Array[String]): Unit = {
        val arrayElement = new ArrayElement(Array("hello", " ", "world"))
        println(arrayElement.width)
        println(arrayElement.height)
        val arrayElementNew = new ArrayElementNew(Array("Hello", " ", "world"))
        println(arrayElementNew.width)
        println(arrayElementNew.height)
        val array = Array("!")
        println(arrayElement.above(array).height)
        println(arrayElement.beside(arrayElementNew).width)
        println("----------------")
        val arrayNewElement = new ElementNewArray(Array("hello", " ", "world"))
        println(arrayNewElement.above(array))
        println(arrayNewElement.beside(arrayNewElement))
    }
}
