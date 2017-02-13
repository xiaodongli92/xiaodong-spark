//package com.xiaodong.spark.examples
//
//import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
//
///**
//  * Created by xiaodong on 2017/1/18.
//  */
//object XGBoostScalaExample {
//    def main(args: Array[String]) {
//        // read trainining data, available at xgboost/demo/data
//        val trainData =
//        new DMatrix("E:\\program\\github\\xiaodong-spark\\spark-examples\\src\\main\\resources\\agaricus.txt.train\\agaricus.txt.train")
//        // define parameters
//        val paramMap = List(
//            "eta" -> 0.1,
//            "max_depth" -> 2,
//            "objective" -> "binary:logistic").toMap
//        // number of iterations
//        val round = 2
//        // train the model
//        val model = XGBoost.train(trainData, paramMap, round)
//        // run prediction
//        val predTrain = model.predict(trainData)
//        // save model to the file.
//        model.saveModel("/local/path/to/model")
//    }
//}
