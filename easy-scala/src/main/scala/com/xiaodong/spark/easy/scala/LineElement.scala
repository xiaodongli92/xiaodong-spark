package com.xiaodong.spark.easy.scala

class LineElement(line: String) extends ArrayElement(Array(line)){

    override def height: Int = line.length

    override def width: Int = 1
}
