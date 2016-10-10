package com.xiaodong.easy.spark

import org.apache.hadoop.fs.FileSystem.Statistics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaodong on 2016/10/9.
  */
object MLLibExample {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MLLibExample").setMaster("local")
        val context = new SparkContext(conf);
        val observations = context.parallelize(
            Seq(
                Vectors.dense(1.0, 10.0, 100.0),
                Vectors.dense(2.0, 20.0, 200.0),
                Vectors.dense(3.0, 30.0, 300.0)
            )
        )
        val summary:MultivariateStatisticalSummary = stat.Statistics.colStats(observations)
        println(summary.mean)
        println(summary.variance)
        println(summary.numNonzeros)
    }
}
