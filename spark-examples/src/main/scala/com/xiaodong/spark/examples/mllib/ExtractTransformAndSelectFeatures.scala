package com.xiaodong.spark.examples.mllib

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by xiaodong on 2017/1/11.
  * 提取、转换和选择特征
  */
object ExtractTransformAndSelectFeatures {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("ExtractTransformAndSelectFeatures")
                .master("local").getOrCreate()
//        exampleTFIDF(spark)
//        word2Vec(spark)
//        countVectorizer(spark)
//        tokenizer(spark)
//        stopWordsRemove(spark)
//        nGram(spark)
//        binarizer(spark)
//        pca(spark)
        polynomialExpansion(spark)
    }

    /**
      * 多项式展开
      * (x,y) degree 设置为2 就会变为 (x, xx, y, xy, yy)
      */
    def polynomialExpansion(spark: SparkSession): Unit = {
        val data = Array(
            Vectors.dense(2.0, 1.0),
            Vectors.dense(0.0, 0.0),
            Vectors.dense(3.0, -1.0)
        )
        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
        val polynomialExpansion = new PolynomialExpansion().setInputCol("features").setOutputCol("poly_features").setDegree(2)
        val polyDF = polynomialExpansion.transform(df)
        polyDF.show(false)

    }

    /**
      * 主成份分析 将多个变量通过线性变换以选出较少个数重要变量的一种多
      */
    def pca(spark:SparkSession): Unit = {
        //将五维特征向量投影到三维中
        val data = Array(
            Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
            Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
            Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
        )
        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
        val pca = new PCA().setInputCol("features").setOutputCol("pca_features").setK(3).fit(df)
        val result = pca.transform(df).select("pca_features")
        result.show(false)

    }

    /**
      * 二值化 threshold是临界值
      * 大于临界值的1
      * 小于临界值的0
      */
    def binarizer(spark: SparkSession): Unit = {
        val data = Array((0,0.1), (1,0.8), (2,0.2))
        val df = spark.createDataFrame(data).toDF("id", "feature")
        val binarizer:Binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarizer_feature")
                .setThreshold(0.5)
        val binarizerDF = binarizer.transform(df)
        println(s"临界值 = ${binarizer.getThreshold}")
        binarizerDF.show(false)
    }

    /**
      * n个单词组合 以空格隔开
      */
    def nGram(spark: SparkSession): Unit = {
        val wordDF = spark.createDataFrame(Seq(
            (0, Array("Hi", "I", "heard", "about", "Spark")),
            (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
            (2, Array("Logistic", "regression", "models", "are", "neat"))
        )).toDF("id", "words")
        val ngram = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams")
        val ngramDF = ngram.transform(wordDF)
        ngramDF.select("ngrams").show(false)
    }

    /**
      * 去停用词
      */
    def stopWordsRemove(spark:SparkSession): Unit = {
        val remover = new StopWordsRemover().setInputCol("row").setOutputCol("filtered")
        val df = spark.createDataFrame(Seq(
            (0, Seq("I", "saw", "THE", "red", "balloon")),
            (1, Seq("Mary", "had", "a", "little", "lamb"))
        )).toDF("id", "row")
        remover.transform(df).show(false)
    }

    /**
      * 分词器
      */
    def tokenizer(spark: SparkSession): Unit = {
        val sentenceDF = spark.createDataFrame(Seq(
            (0, "Hi I heard about Spark"),
            (1, "I wish Java could use case classes"),
            (2, "Logistic,regression,models,are,neat")
        )).toDF("id", "sentence")
        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words")
                .setPattern("\\W")
        val countTokens = udf{(words:Seq[String]) => words.length}

        val tokenized = tokenizer.transform(sentenceDF)
        tokenized.select("sentence", "words").withColumn("tokens", countTokens(col("words"))).show(false)

        val regexTokenized = regexTokenizer.transform(sentenceDF)
        regexTokenized.select("sentence", "words").withColumn("tokens", countTokens(col("words"))).show(false)
    }

    /**
      * CountVectorizer算法是将文本向量转换成稀疏表示的数值向量
      */
    def countVectorizer(spark: SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (0, Array("a", "b", "c")),
            (1, Array("a", "b", "b", "b", "c", "a"))
        )).toDF("id", "words")
        val cvModel:CountVectorizerModel = new CountVectorizer()
                .setInputCol("words")
                .setOutputCol("features")
                .setVocabSize(3)
                .setMinDF(2)
                .fit(df)

        val cvm = new CountVectorizerModel(Array("a", "b", "c"))
                .setInputCol("words")
                .setOutputCol("features")
        cvModel.transform(df).show(false)
    }

    //单词转换向量
    def word2Vec(spark: SparkSession): Unit = {
        val documentDF = spark.createDataFrame(Seq(
            "Hi I heard about Spark".split(" "),
            "I wish Java could use case classes".split(" "),
            "Logistic regression models are neat".split(" ")
        ).map(Tuple1.apply)).toDF("text")

        val word2Vec = new Word2Vec().setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)//目标数值向量的纬度大小，默认是100
                .setMinCount(0)//只有当某个词出现的次数大于或者等于minCount时，才会被包含在词汇表中，否则会被忽略
                .setNumPartitions(1)//训练数据的分区数 默认是1
                .setMaxIter(1)//算法求最大迭代次数，小于等于分区数 默认是1
                .setStepSize(0.025)//优化算法的每一次迭代的学习速率，默认是0.025
        val model = word2Vec.fit(documentDF)
        val result = model.transform(documentDF)
        result.collect().foreach{
            case Row(text: Seq[_], features: Vector) =>
                println(s"Text:[${text.mkString(" ")}] => \nVector:$features\n")
        }
    }

    /**
      * TF-IDF是一个广泛用于文本挖掘的特征矢量化方法，用来反映语料库的文档的术语的重要性
      */
    def exampleTFIDF(spark: SparkSession): Unit = {
        val sentenceData = spark.createDataFrame(Seq(
            (0.0, "Hi I heard about Spark"),
            (0.0, "I wish Java could use case classes"),
            (1.0, "Logistic regression models are neat")
        )).toDF("label", "sentence")
        //将文本分割为单词
        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData = tokenizer.transform(sentenceData)
        wordsData.printSchema()
        //特征哈希 用于计算词频
        val hashingTf = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
        val featurizedData = hashingTf.transform(wordsData)
        featurizedData.printSchema()
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featurizedData)
        val rescaledData = idfModel.transform(featurizedData)
        rescaledData.select("label","features").show(false)
        rescaledData.printSchema()
    }
}
