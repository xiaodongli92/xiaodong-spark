package com.xiaodong.spark.easy.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lixiaodong on 16/9/30.
  */
object MaxLine {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("MaxLine")
        conf.setMaster("local")
        val context = new SparkContext(conf)
        val lines = context.textFile("/Users/lixiaodong/Documents/enviroment/spark-2.0.0-bin-hadoop2.7/README.md")
        val linesAndSize = lines.map(line => (line.split(" ").length, line))
        println(linesAndSize.collect().mkString)
        println(linesAndSize.sortByKey().collect().mkString)
        println(linesAndSize.max())
    }
}
