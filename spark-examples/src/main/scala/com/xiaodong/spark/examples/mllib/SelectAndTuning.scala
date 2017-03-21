package com.xiaodong.spark.examples.mllib

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xiaodong on 2017/3/17.
  * 数据量小的时候用CrossValidator进行交叉验证
  * 数据量大的时候用trainValidationSplit
  */
object SelectAndTuning {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("SelectAndTuning")
                .getOrCreate()
//        crossValidation(spark)
        trainValidationSplit(spark)
    }

    /**
      * 交叉验证
      */
    def crossValidation(spark:SparkSession): Unit = {
        val training = spark.createDataFrame(Seq(
            (0L, "a b c d e spark", 1.0),
            (1L, "b d", 0.0),
            (2L, "spark f g h", 1.0),
            (3L, "hadoop mapreduce", 0.0),
            (4L, "b spark who", 1.0),
            (5L, "g d a y", 0.0),
            (6L, "spark fly", 1.0),
            (7L, "was mapreduce", 0.0),
            (8L, "e spark program", 1.0),
            (9L, "a e c l", 0.0),
            (10L, "spark compile", 1.0),
            (11L, "hadoop software", 0.0)
        )).toDF("id", "text", "label")
        val tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("word")
        val hashingTF = new HashingTF()
                .setInputCol(tokenizer.getOutputCol)
                .setOutputCol("features")
        val lr = new LogisticRegression()
                .setMaxIter(10)
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

        val paramGrid = new ParamGridBuilder()
                .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
                .addGrid(lr.regParam, Array(0.1, 0.01))
                .build()

        val cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new BinaryClassificationEvaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(2)

        val cvModel = cv.fit(training)

//        val bestPipeline:Pipeline = cvModel.bestModel.parent.asInstanceOf[Pipeline]
//        val lrModel = bestPipeline.getStages(2).asInstanceOf[LogisticRegression]
//        println(lrModel.extractParamMap())
        val pipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
        val lrModel = pipelineModel.stages(2)
        println(lrModel.extractParamMap())

        val test = spark.createDataFrame(Seq(
            (4L, "spark i j k"),
            (5L, "l m n"),
            (6L, "mapreduce spark"),
            (7L, "apache hadoop")
        )).toDF("id", "text")

        cvModel.transform(test)
                .select("id", "text", "probability", "prediction")
                .collect()
                .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
                    println(s"($id, $text) --> prob=$prob, prediction=$prediction")
                }
    }

    def trainValidationSplit(spark:SparkSession): Unit = {
        val data = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_linear_regression_data.txt")
        val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1), seed = 12345)
        val lr = new LinearRegression().setMaxIter(10)
        val paramGrid = new ParamGridBuilder()
                .addGrid(lr.regParam, Array(0.1, 0.01))
                .addGrid(lr.fitIntercept)
                .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
                .build()
        val trainValidationSplit = new TrainValidationSplit()
                .setEstimator(lr)
                .setEvaluator(new RegressionEvaluator)
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8)
        val model = trainValidationSplit.fit(trainingData)

        println(model.bestModel.extractParamMap())
        model.transform(testData).select("features", "label", "prediction").show()
    }

}
