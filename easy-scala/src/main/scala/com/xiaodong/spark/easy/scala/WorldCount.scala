package com.xiaodong.spark.easy.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lixiaodong on 16/8/30.
  */
object WorldCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("word count")
    conf.setMaster("local")
    val context = new SparkContext(conf)
    val lines = context.textFile("/Users/lixiaodong/Documents/JavaWordCount.java")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.saveAsTextFile("/Users/lixiaodong/Documents/output/noSort")
    result.sortByKey().saveAsTextFile("/Users/lixiaodong/Documents/output/sort/")
    result.collect().foreach(println)
    context.stop()
  }
}
