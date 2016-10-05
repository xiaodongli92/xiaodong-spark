package com.xiaodong.spark.easy.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lixiaodong on 16/10/5.
  */
object StreamingWordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("StreamingWordCount")
        //1秒批间隔时间
        val ssc = new StreamingContext(conf, Seconds(1))
        val lines = ssc.socketTextStream("127.0.0.1", 9999)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()
        ssc.start()//开始计算
        ssc.awaitTermination()//等待计算终止
    }
}
