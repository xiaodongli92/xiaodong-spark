package com.xiaodong.spark.easy.scala

class Rational(x: Int, y: Int) {
    require(y != 0)

    private val greatestCommonDivisor : Int = greatestCommonDivisor(x.abs, y.abs)
    val numerator: Int = x / greatestCommonDivisor
    val denominator: Int = y / greatestCommonDivisor

    def this(x: Int) = this(x, 1)

    def +(that: Rational) = add(that)

    def -(that: Rational) = {
        new Rational(numerator * that.denominator - denominator * that.numerator, denominator * that.denominator)
    }

    def *(that: Rational) = {new Rational(numerator * that.numerator, denominator * that.denominator)}

    def /(that: Rational) = {
        require(that.numerator != 0)
        new Rational(numerator * that.denominator, denominator * that.numerator)
    }

    def add(that: Rational): Rational = {
        new Rational(numerator * that.denominator + that.numerator * denominator, denominator * that.denominator)
    }

    def lessThan(that: Rational) = {
        numerator * that.denominator < denominator * that.numerator
    }

    def max(that: Rational) = {
        if (lessThan(that)) that else this
    }

    private def greatestCommonDivisor(x: Int, y: Int) : Int = {
        if (y == 0) x else greatestCommonDivisor (y, x%y)
    }

    override def toString: String = {
        if (numerator == 0) {
            "0"
        } else if (denominator == 1) {
            numerator.toString
        } else {
            numerator + "/" + denominator
        }
    }
}
