package com.xiaodong.spark.examples.mllib

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.max
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/**
  * Created by lixiaodong on 17/2/16.
  * 分类和回归
  */
object ClassificationAndRegression {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("ClassificationAndRegression")
          .master("local")
          .getOrCreate()
//        binomialLogisticRegression(spark)
//        multinomialLogisticRegression(spark)
        decisionTreeClassifier(spark)
    }

    /**
      * 决策树分类器
      */
    def decisionTreeClassifier(spark:SparkSession): Unit = {
        val data = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_libsvm_data.txt")

        //索引标签,向标签列添加元数据
        //适合整个数据集,以包括索引中的所有标签
        val labelIndex = new StringIndexer()
          .setInputCol("label")
          .setOutputCol("indexedLabel")
          .fit(data)
        //自动识别分类特征,并对他们建立索引
        val featureIndexer = new VectorIndexer()
          .setInputCol("features")
          .setOutputCol("indexedFeatures")
          .setMaxCategories(4)//具有大于4个不同值得要素被视为连续
          .fit(data)
        //将数据差费成训练和测试集(30%留出测试)
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
        //训练决策树模型
        val dt = new DecisionTreeClassifier()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("indexedFeatures")

        //将索引标签转换回原始标签
        val labelConverter = new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("predictedLabel")
          .setLabels(labelIndex.labels)

        val pipeline = new Pipeline()
          .setStages(Array(labelIndex, featureIndexer, dt, labelConverter))

        val model = pipeline.fit(trainingData)

        //进行预测
        val predictions = model.transform(testData)

        predictions.select("predictedLabel", "label", "features").show(5)

        //选择(预测、标签)和计算测试错误
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(predictions)

        println("test error = " + (1.0 - accuracy))

        val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        println("学习分类树模型:\n" + treeModel.toDebugString)

    }

    /**
      * 多项式逻辑回归
      */
    def multinomialLogisticRegression(spark:SparkSession): Unit = {
        val training = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_multiclass_classification_data.txt")
        val lr = new LogisticRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)

        val lrModel = lr.fit(training)
        println(s"系数:\n${lrModel.coefficientMatrix}")
        println(s"截距:\n${lrModel.interceptVector}")
    }

    /**
      * logistic regression 逻辑回归
      * 逻辑回归是预测分类响应的流行方法。是预测结果概率的广义线性模型的特殊情况
      * binomial logistic regression 二项式逻辑回归 预测二元结果
      */
    def binomialLogisticRegression(spark:SparkSession): Unit = {
        import spark.implicits._
        val training = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_libsvm_data.txt")

        //设置模型参数
        val lr = new LogisticRegression()
          .setMaxIter(10)//设置最大迭代步数
          .setRegParam(0.3)//对应公式的λ,
          .setElasticNetParam(0.8)//对应公式的α
          .setThreshold(0.5)//控制分类的阈值,默认值为0.5,如果预测值小于threshold分类为0.0,否则为1.0

        val lrModel = lr.fit(training)
        println(s"二项式逻辑回归系数:${lrModel.coefficients}\n截距:${lrModel.intercept}")

        val mlr = new LogisticRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)
          .setFamily("multinomial")
        val mlrModel = mlr.fit(training)
        println(s"多项式 系数:${mlrModel.coefficientMatrix}")
        println(s"多项式 截距:${mlrModel.interceptVector}")

        //从之前训练返回的实例提取摘要
        val trainingSummary = lrModel.summary
        //每次迭代获取目标
        val objectiveHistory = trainingSummary.objectiveHistory
        println("objectiveHistory:")
        objectiveHistory.foreach(println)

        val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
        val roc = binarySummary.roc
        roc.show(false)
        println(s"areaUnderROC:${binarySummary.areaUnderROC}")

        //设置模型阈值以最大化F-Measure
        val fMeasure = binarySummary.fMeasureByThreshold
        val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)

        val bestThreshold = fMeasure.where($"F-Measure"===maxFMeasure).select("threshold").head().getDouble(0)
        lrModel.setThreshold(bestThreshold)
        println(s"""best threshold =$bestThreshold""")
    }

}
