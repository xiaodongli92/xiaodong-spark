package com.xiaodong.spark.easy.scala

object RationalExample {

    def main(args: Array[String]) {
        val twoThirds = new Rational(2, 3)
        val oneHalf = new Rational(1, 2)
        println(oneHalf + twoThirds)
        println(oneHalf - twoThirds)
        println(twoThirds - oneHalf)
        println(oneHalf * twoThirds)
        println(oneHalf / twoThirds)
        println(new Rational(2) * oneHalf)
        println(new Rational(0))
    }
}
