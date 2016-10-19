package com.xiaodong.easy.spark

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat
import org.apache.spark.mllib.stat.test.{BinarySample, ChiSqTestResult, StreamingTest}
import org.apache.spark.mllib.stat.{KernelDensity, MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaodong on 2016/10/9.
  */
object MLLibExample {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MLLibExample").setMaster("local")
        val context = new SparkContext(conf);
//        val streamingContext = new StreamingContext(conf, Seconds(1))
//        summaryStatistics(context)
//        correlations(context)
//        stratifiedSampling(context);
//        hypothesisTesting(context)
//        kolmogorovSmirnovTest(context)
//        streamingSignificanceTesting(streamingContext)
//        randomDataGeneration(context)
        kernelDensityEstimation(context)
    }

    /**
      * 核密度估计
      */
    private def kernelDensityEstimation(context:SparkContext): Unit = {
        val data:RDD[Double] = context.parallelize(Seq(1,1,1,2,3,4,5,6,7,8,9,9))
        println(data.collect().mkString)
        val kd = new KernelDensity().setSample(data)//设置密度估计样本
                .setBandwidth(3.0)//设置带宽，对高斯核函数来讲就是标准差
        val densities = kd.estimate(Array(-1.0, 2.0, 5.0, 6.0, 7.0, 9.0))//给定相应的点，估计其概率密度
        println(densities.mkString)
    }

    /**
      * 随机数
      */
    private def randomDataGeneration(context:SparkContext): Unit = {
        val u = RandomRDDs.normalRDD(context, 100L, 10)
        println("原始：" + u.collect().mkString)
        val v = u.map(x => 1.0 + 2.0 * x)
        println("map后" + v.collect().mkString)
    }

    /**
      * 显著性检验就是事先对总体形式做出一个假设，然后用样本信息来判断这个假设（原假设）是否合理，即判断真实情况与原假设是否显著地有差异。
      * 或者说，显著性检验要判断样本与我们对总体所做的假设之间的差异是否纯属偶然，还是由我们所做的假设与总体真实情况不一致所引起的
      * peacePeriod: 用于设置一段冷静期来忽略stream中开头的一段脏数据。
      * windowSize: 采集窗口，如果给0，则不分批次一次处理全量数据
      */
    private def streamingSignificanceTesting(context: StreamingContext): Unit = {
        context.checkpoint("E:\\program\\github\\spark_data\\")
        val path = "E:\\program\\github\\spark_data\\streamingSignificanceTesting.txt"
        val data = context.textFileStream(path).map(line => line.split(",") match {
            case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
        })
        data.print()
        val streamingTest = new StreamingTest().setPeacePeriod(0).setWindowSize(0).setTestMethod("welch")
        val out = streamingTest.registerStream(data)
        out.print()
        context.start()
        context.awaitTermination()
    }

    /**
      * Kolmogorov-Smirnov 即 K-S test 即 柯尔莫可洛夫-斯米洛夫检验
      * 柯尔莫可洛夫-斯米洛夫检验基于累计分布函数，用以检验两个经验分布是否不同或一个经验分布与另一个理想分布是否不同
      * 采用柯尔莫诺夫-斯米尔诺夫检验来分析变量是否符合某种分布，可以检验的分布有正态分布、均匀分布、Poission分布和指数分布
      * @param context
      */
    private def kolmogorovSmirnovTest(context: SparkContext): Unit = {
        val data:RDD[Double] = context.parallelize(Seq(0.1, 0.15, 0.2, 0.3, 0.25))
        /**
          * data：    待检测样本
          * "norm"：  理论分布类型，默认是检测标准正态分布
          * 0：       ALPHA是显著性水平（默认0.05）
          * 1：       H=1 则否定无效假设； H=0，不否定无效假设（在alpha水平上）
          */
        val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
        println(testResult)
        println
        val myCDF = Map(0.1 -> 0.2, 0.15 -> 0.6, 0.2 -> 0.05, 0.3 -> 0.05, 0.25 -> 0.1)
        /**
          * 检验两个数据向量之间的分布
          */
        val testResult2 = Statistics.kolmogorovSmirnovTest(data, myCDF)
        println(testResult2)
    }

    /**
      * 假设校验
      * spark通过 Statistics 类来支持 Pearson's chi-squared （卡方检测），
      * 主要是比较两个及两个以上样本率( 构成比）以及两个分类变量的关联性分析。
      * 其根本思想就是在于比较理论频数和实际频数的吻合程度或拟合优度问题。
      * 卡方检测有两种用途，分别是“适配度检定”（Goodness of fit）以及“独立性检定”（independence）
      *
      * Goodness fo fit（适合度检验）： 执行多次试验得到的观测值，与假设的期望数相比较，观察假设的期望值与实际观测值之间的差距，
      * 称为卡方适合度检验，即在于检验二者接近的程度。比如掷色子
      *
      * Indenpendence(独立性检验)： 卡方独立性检验是用来检验两个属性间是否独立。
      * 其中一个属性做为行，另外一个做为列，通过貌似相关的关系考察其是否真实存在相关性。比如天气温变化和肺炎发病率。
      *
      * 假定检测的基本思路是，首先我们假定一个结论，然后为这个结论设置期望值，
      * 用实际观察值来与这个值做对比，并设定一个阀值，如果计算结果大于阀值，则假定不成立，否则成立
      *
      * 1) 结论：结论一般是建立在零假设( Null Hypothesis)的基础上的。零假设即认为观测值与理论值的差异是由于随机误差所致。
            比如：“掷色子得到的各种结果概率相同”——这个结论显然我们认定的前提是即便不同也是随机因素导致。
        2) 期望值：期望值也就是理论值，理论值可以是某种平均数，比如我们投掷120次色子，要维护结论正确，那么每个数字的出现理论值就应该都是20
        3) 观测值：也就是实际得到的值
        4) 阀值：阀值是根据自由度和显著性水平计算出来的（excel 中的 chiinv() 函数）。自由度=(结果选项数-1)x(对比组数-1)，
            比如我们将两组掷色子值做比较，那么自由度就是(6-1)x(2-1)=5。显著性水平(a)是原假设为正确的，而我们确把原假设当做错误加以拒绝，
            犯这种错误的概率，依据拒绝区间所可能承担的风险来决定，一般选择0.05或0.01。

      * 最后就是计算卡方值：卡方值是各组 （观测值－理论值）的平方/理论值  的总和。最后就是比较方差值和阀值。
      * 如果小于阀值则接受结论，否则拒绝结论。或者根据卡方值反算概率p值(excel 中的 chidist() 函数)，将它和显著性水平比较，小于则拒绝，大于则接受
      */
    private def hypothesisTesting(context: SparkContext): Unit = {
        val vector: Vector = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25)
        val goodnessOfFitTestResult = Statistics.chiSqTest(vector)
        println("向量假设校验")
        println("方法：" + goodnessOfFitTestResult.method)
        println("自由度：" + goodnessOfFitTestResult.degreesOfFreedom)
        println("卡方值：" + goodnessOfFitTestResult.statistic)
        println("拒绝原假设的最小显著性水平：" + goodnessOfFitTestResult.pValue)
        val matrix: Matrix = Matrices.dense(2, 3, Array(1, 3, 5, 2, 4, 6))
        val independenceTestResult = Statistics.chiSqTest(matrix)
        println("矩阵假设校验")
        println(independenceTestResult)
        val obs: RDD[LabeledPoint] = context.parallelize(Seq(
            LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
            LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
            LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5))
        ))
        val featureTestResults:Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
        println("RDD假设校验")
        featureTestResults.zipWithIndex.foreach { case (k, v) =>
                println("Column " + (v + 1).toString + ":" + k)
        }
    }

    /**
      * 分层抽样
      * sampleByKey 并不对过滤全量数据，因此只得到近似值
      * sampleByKeyExtra 会对全量数据做采样计算，因此耗费大量的计算资源，但是结果会更准确
      */
    private def stratifiedSampling(context: SparkContext): Unit = {
        val data = context.parallelize(Seq(
            (1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')
        ))
        val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)
        //每个层中大概的例子
        val approxSample = data.sampleByKey(withReplacement = false, fractions = fractions)
        println("每个层中大概的例子:" + approxSample.collect().mkString)
        //每个层中准确的例子
        val exactSample = data.sampleByKeyExact(withReplacement = false, fractions = fractions)
        println("每个层中准确的例子:" + exactSample.collect().mkString)
    }

    /**
      * 相关性统计
      *
      * 皮尔森相关：是一种线性相关，用来反映两个变量线性相关程度的统计量 用来衡量两个数据（集）是否在同一个层面上
      * 相关系数的绝对值越大，相关性越强：相关系数越接近于1或-1，相关度越强，相关系数越接近于0，相关度越弱
      *
      * 0.8-1.0 极强相关
        0.6-0.8 强相关
        0.4-0.6 中等程度相关
        0.2-0.4 弱相关
        0.0-0.2 极弱相关或无相关
      *
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
