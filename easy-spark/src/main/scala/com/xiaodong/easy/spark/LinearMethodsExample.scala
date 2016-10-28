package com.xiaodong.easy.spark

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

/**
  * 线性相关
  */
object LinearMethodsExample {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("LinearMethodsExample")
        val context = new SparkContext(conf)
        testSVMs(context)
    }

    /**
      * 线性支持向量机
      * L(w;x,y):=max{0,1−ywTx}
      */
    private def testSVMs(context: SparkContext): Unit = {
        val data = MLUtils.loadLibSVMFile(context, "easy-spark/src/main/resources/sample_libsvm_data.txt")
        println("原始数据：" + data.collect().mkString)
        val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        //训练样本
        val training = splits(0).cache()
        //测试样本
        val test = splits(1)
        //迭代次数
        val numIterations = 100
        //利用SGD方法使用凸优化的方法来对目标函数进行优化
        val model = SVMWithSGD.train(training, numIterations)
        //清除默认阈值
        model.clearThreshold()
        val scoreAndLabels = test.map{point =>
            val score = model.predict(point.features)
            (score, point.label)
        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()
        println("area and ROC = " + auROC)

//        model.save(context, "target/tmp/SVMWithSGDModel")
//        val sameModel = SVMModel.load(context, "target/tmp/SVMWithSGDModel")
    }
}
