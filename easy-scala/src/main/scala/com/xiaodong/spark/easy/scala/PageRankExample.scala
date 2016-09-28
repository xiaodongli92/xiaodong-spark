package com.xiaodong.spark.easy.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lixiaodong on 16/9/28.
  *
  * v′=αMv+(1−α)e
  *
  * α 当前页面的概率 1-α 跳转到其他页面的概率
  * M 网页跳转转移矩阵
  * v初始rank
  * v′ 最终rank
  *
  */
object PageRankExample {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("PageRange")
        conf.setMaster("local")
        val context = new SparkContext(conf)
        val lines = context.textFile("/Users/lixiaodong/Documents/test/pagerank.txt")
        val linkes = lines.map { s =>
            val parts = s.split(" ")
            (parts(0), parts(1))
        }.distinct().groupByKey().cache()
        linkes.foreach(println)
        var ranks = linkes.mapValues(v => 1.0)
        ranks.foreach(println)
        for (i <- 1 to 10) {
            val contribs = linkes.join(ranks).values.flatMap { case (urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            contribs.foreach(println)
            ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85*_)
        }
        val output = ranks.sortByKey().collect()
        output.foreach(tup => println(tup._1 + " has rank:" + tup._2))
        var count:Double = 0
        output.foreach(tup => count += tup._2)
        println(count)
    }
}
