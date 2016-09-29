package com.xiaodong.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lixiaodong on 16/9/29.
  */
object DriverSubmissionTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("DriverSubmissionTest")
        conf.setMaster("local")
        val context = new SparkContext(conf)

    }
}
