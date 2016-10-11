package com.xiaodong.easy.spark

import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaodong on 2016/10/9.
  */
object MLLibExample {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MLLibExample").setMaster("local")
        val context = new SparkContext(conf);
//        summaryStatistics(context)
        correlations(context)
    }

    /**
      * 相关性统计
      *
      * 皮尔森相关：是一种线性相关，用来反映两个变量线性相关程度的统计量
      * 斯波尔曼等级相关：主要用于解决称名数据和顺序数据相关的问题，适用于两列变量，而且具有等级线性关系的资料
      * 这两个相关统计，都是在计算基因共表达或多组学贯穿分析时常用的相关性度量方法。
      * 因为基因间调控方式可能并非线性，加上实验误差、检测误差等因素的干扰，皮尔森相关的显著性可能会下降，但是斯波尔曼等级相关可以弥补
      * 因此并不能武断决定哪种相关性计算方式最佳，根据具体情况定制个性化的分析策略
      */
    private def correlations(context: SparkContext): Unit = {
        val serialX: RDD[Double] = context.parallelize(Array(1, 2, 3, 4, 5))
        val serialY: RDD[Double] = context.parallelize(Array(11, 22, 33, 44, 555))
        val serialZ: RDD[Double] = context.parallelize(Array(1, 2, 3, 4, 5))
        val serialU: RDD[Double] = context.parallelize(Array(11, 22, 33, 44, 55))
        val correlation1:Double = Statistics.corr(serialX, serialY, "pearson")
        val correlation2:Double = Statistics.corr(serialX, serialZ, "pearson")
        val correlation3:Double = Statistics.corr(serialX, serialU, "pearson")
        val correlation4:Double = Statistics.corr(serialX, serialY, "spearman")
        val correlation5:Double = Statistics.corr(serialX, serialZ, "spearman")
        val correlation6:Double = Statistics.corr(serialX, serialU, "spearman")
        println("1,2,3,4,5 和 11,22,33,44,555 pearson比较：" + correlation1)
        println("相同的两个数组pearson比较：" + correlation2)
        println("1,2,3,4,5 和 11,22,33,44,55 pearson比较：" + correlation3)
        println("1,2,3,4,5 和 11,22,33,44,555 spearman比较：" + correlation4)
        println("相同的两个数组spearman比较：" + correlation5)
        println("1,2,3,4,5 和 11,22,33,44,55 spearman比较：" + correlation6)
        val data:RDD[Vector] = context.parallelize(Seq(
            Vectors.dense(1.0, 10.0, 100.0),
            Vectors.dense(2.0, 20.0, 200.0),
            Vectors.dense(5.0, 33.0, 366.0)
        ))
        val correlationMatrix: Matrix = Statistics.corr(data, "pearson")
        println(correlationMatrix)
    }

    /**
      * 概要统计
      */
    private def summaryStatistics(context: SparkContext): Unit = {
        val observations = context.parallelize(
            Seq(
                Vectors.dense(1.0, 10.0, 100.0),
                Vectors.dense(2.0, 20.0, 200.0),
                Vectors.dense(3.0, 30.0, 300.0)
            )
        )
        val summary:MultivariateStatisticalSummary = stat.Statistics.colStats(observations)
        println("平均值：" + summary.mean)
        println("方差：" + summary.variance)
        println("非零统计量的个数：" + summary.numNonzeros)
        println("总数：" + summary.count)
        println("最大值：" + summary.max)
        println("最小值：" + summary.min)
        println("" + summary.normL1)
        println("" + summary.normL2)
    }
}
