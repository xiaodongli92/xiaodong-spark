package com.xiaodong.spark.easy.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lixiaodong on 16/8/31.
  */
object SpecificWorkCount {

  def main(args: Array[String]): Unit = {
    val inputFile = "/Users/lixiaodong/Documents/input/specificWordCount.txt"
    val conf = new SparkConf().setAppName("Specific World Count").setMaster("local")
    val sparkContext = new SparkContext(conf);
    val data = sparkContext.textFile(inputFile).cache()
    val numO = data.filter(line => line.contains("o")).count()
    val numL = data.filter(line => line.contains("l")).count()
    println("numO = %s;numL = %s".format(numO, numL))
  }
}
