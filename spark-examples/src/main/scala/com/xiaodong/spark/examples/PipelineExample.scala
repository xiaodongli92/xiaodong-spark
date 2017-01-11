package com.xiaodong.spark.examples

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel, feature}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xiaodong on 2017/1/11.
  */
object PipelineExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("pipeline").master("local").getOrCreate()
//        estimatorTransformerAndParam(spark)
        pipelineExample(spark)
    }

    def pipelineExample(spark: SparkSession): Unit = {
        val training = spark.createDataFrame(Seq(
            (0L, "a b c d e spark", 1.0),
            (1L, "b d", 0.0),
            (2L, "spark f g h", 1.0),
            (3L, "hadoop mapreduce", 0.0)
        )).toDF("id", "text", "label")
        val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
        val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
        val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
        val model = pipeline.fit(training)
        model.write.overwrite().save("/tmp/spark-logistic-regression-model")
        pipeline.write.overwrite().save("/tmp/unfit-lr-model")
        val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

        val test = spark.createDataFrame(Seq(
            (4L, "spark i j k"),
            (5L, "l m n"),
            (6L, "spark hadoop spark"),
            (7L, "apache hadoop")
        )).toDF("id", "text")

        model.transform(test).select("id","text","probability","prediction").collect()
                .foreach{
                    case Row(id:Long, text:String, prob:Vector, prediction:Double) =>
                        println(s"($id,$text) --> prob=$prob, prediction=$prediction")
                }
    }

    /**
      * 估计器、变换器和参数
      */
    def estimatorTransformerAndParam(spark: SparkSession): Unit = {
        val training = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(0.0, 1.0, 0.1)),
            (0.0, Vectors.dense(2.0, 1.0, -1.0)),
            (0.0, Vectors.dense(2.0, 1.3, 1.0)),
            (1.0, Vectors.dense(0.0, 1.2, -0.5))
        )).toDF("label", "features")
        val lr = new LogisticRegression()
        println("逻辑回归参数=" + lr.explainParams())

        //重新设置模型参数方法
        lr.setMaxIter(10)//设置最大迭代步数
                .setRegParam(0.01)

        val model1 = lr.fit(training)
        println("model1 fit params = " + model1.parent.explainParams())

        //使用ParamMap重新设置参数
        val paramMap = ParamMap(lr.maxIter -> 20).put(lr.maxIter -> 30)
                .put(lr.regParam->0.1, lr.threshold -> 0.55)

        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")//改变输出列的名字
        val paramMapCombined = paramMap ++ paramMap2

        val model2 = lr.fit(training, paramMapCombined)
        println("model2 fit params = " + model2.parent.explainParams())

        val test = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
            (0.0, Vectors.dense(3.0, 2.0, -0.1)),
            (1.0, Vectors.dense(0.0, 2.2, -1.5))
        )).toDF("label", "features")
        model2.transform(test).select("features", "label", "myProbability", "prediction")
                .collect()
                .foreach{
                    case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
                        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
                }
    }
}
