package com.xiaodong.spark.easy.scala

import ElementNew.elem

abstract class ElementNew {

    def contents: Array[String]

    def height: Int = contents.length

    def width: Int = if (height == 0) 0 else contents(0).length

    def above(that: Array[String]): ElementNew = elem(this.contents ++ that)

    def beside(that: ElementNew): ElementNew = elem(
        for ((line1, line2) <- this.contents zip that.contents)
            yield line1 + line2
    )

    override def toString: String = contents mkString "\n"
}

object ElementNew {

    def elem(contents: Array[String]):ElementNew = new ElementNewArray(contents)
}
