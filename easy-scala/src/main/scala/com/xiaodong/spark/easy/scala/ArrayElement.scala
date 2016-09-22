package com.xiaodong.spark.easy.scala


class ArrayElement(cont: Array[String]) extends Element{

    override def contents: Array[String] = cont

    def above(that: Array[String]): Element = new ArrayElement(contents ++ that)

    def beside(that: Element): Element = {
        val contents = new Array[String](this.contents.length)
        for (i <- this.contents.indices)
            contents(i) = this.contents(i) + that.contents(i)
        new ArrayElement(contents)
    }
}
