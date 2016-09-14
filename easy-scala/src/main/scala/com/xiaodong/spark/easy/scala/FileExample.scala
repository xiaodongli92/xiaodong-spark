package com.xiaodong.spark.easy.scala

import scala.io.Source

/**
  * Created by xiaodong on 2016/9/12.
  */
object FileExample {
    def main(args: Array[String]): Unit = {
        val path = "E:\\Workspace\\github\\xiaodong-spark\\easy-scala\\src\\main\\scala\\com\\xiaodong\\spark\\easy\\scala\\ForExample.scala"
        //    readFile1(path)
        readFile2(path)
    }

    def readFile1(path: String): Unit = {
        for (line <- Source.fromFile(path).getLines()) {
            println(line.length + "\t" + line)
        }
    }

    def readFile2(path: String): Unit = {
        val lines = Source.fromFile(path).getLines().toList
        var maxWidth = 0
        val maxRow = lines.size.toString.length
        for (line <- lines) {
            maxWidth = maxWidth.max(widthOfLength(line))
        }
        var row = 0
        for (line <- lines) {
            row += 1
            val numSpace = maxWidth - widthOfLength(line)
            val padding = " " * numSpace
            val rowSpace = maxRow - row.toString.length
            val rowPadding = " " * rowSpace
            println(rowPadding + row + " | " + padding + line.length + " | " + line)
        }
    }

    def widthOfLength(s: String) = s.length.toString.length
}
