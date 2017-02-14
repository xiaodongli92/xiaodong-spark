package com.xiaodong.spark.examples.mllib

import org.apache.spark.ml.attribute.Attribute
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
//        polynomialExpansion(spark)
//        discreteCosineTransform(spark)
//        stringIndexer(spark)
//        index2String(spark)
//        oneHotEncoder(spark)
//        vectorIndexer(spark)
//        interaction(spark)
//        normalizer(spark)
//        standardScaler(spark)
//        minMaxScaler(spark)
//        maxAbsScaler(spark)
//        bucketizer(spark)
//        elementwiseProduct(spark)
//        sqlTransformer(spark)
        vectorAssembler(spark)
    }

    /**
      * 向量组合
      * 将给定的列列表组合成单个向量的变换器
      * 有助于将由不同特征变换器生成的原始特征和特征组合成单个特征向量,以训练ML模型,如逻辑回归和决策树
      * 接受类型:所有数字类型、布尔类型和向量类型
      */
    def vectorAssembler(spark:SparkSession): Unit = {
        val dataset = spark.createDataFrame(Seq(
            (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0)
        )).toDF("id", "hour", "mobile", "userFeatures", "clicked")

        val assembler = new VectorAssembler()
          .setInputCols(Array("hour", "mobile", "userFeatures"))
          .setOutputCol("features")

        val output = assembler.transform(dataset)
        println("assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
        output.show(false)
    }

    /**
      * sql转换器
      * 实现由sql语句定义的变换
      * 目前只支持的语法:select from _this_,where _this_ 表示数据集的基础表
      * select子句指定要在输出中显示的字段、常量和表达式,并且可以是spark sql支持的人格select子句
      * 用户还可以使用spark sql内置函数和udf操作这些选定的列
      */
    def sqlTransformer(spark:SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (0, 1.0, 2.0),
            (2, 2.0, 5.0)
        )).toDF("id", "v1", "v2")
        val sqlTrans = new SQLTransformer()
          .setStatement("select *,(v1+v2) as v3, (v1*v2) as v4 from __THIS__")
        sqlTrans.transform(df).show(false)
    }

    /**
      * 元素乘积
      * 使用元素级乘法将每个输入向量乘以提供的权重向量。
      * 通过标量乘法器来缩放数据集的每一列
      * hadamard乘积
      * (v1...vn) . (w1...wm) = (v1w1...vnwm)
      */
    def elementwiseProduct(spark:SparkSession): Unit = {
        val dataFrame = spark.createDataFrame(Seq(
            ("a", Vectors.dense(1.0, 2.0, 3.0)),
            ("b", Vectors.dense(4.0, 5.0, 6.0))
        )).toDF("id", "vector")
        val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
        val transformer = new ElementwiseProduct()
          .setScalingVec(transformingVector)
          .setInputCol("vector")
          .setOutputCol("transformedVector")

        transformer.transform(dataFrame).show(false)
    }

    /**
      * 将连续要素列转换为要素桶列,其中存储桶由用户指定
      * splits:用于将连续要素映射到bucket的参数,对于n+1分割,有n个桶。由于分割x,y定义的桶保存除了最后一个桶之外的范围[x,y)中的值
      * splits应该严格增加,必须明确提供负无穷和正无穷的值
      */
    def bucketizer(spark:SparkSession): Unit = {
        val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
        val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9, 10, 0.5)
        val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

        val bucketizer = new Bucketizer()
          .setInputCol("features")
          .setOutputCol("bucketedFeatures")
          .setSplits(splits)
        val bucketdData = bucketizer.transform(dataFrame)

        println(s"bucketizer output with ${bucketizer.getSplits.length} buckets")
        bucketdData.show(false)
    }

    /**
      * 转换Vector行的数据集
      * 通过划分每个要素中的最大绝对值,将每个元素重新缩放到范围[-1,1]
      * 不会移动/居中数据,因此不会破坏任何稀疏性
      */
    def maxAbsScaler(spark:SparkSession): Unit = {
        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.1, -8.0)),
            (1, Vectors.dense(2.0, 1.0, -4.0)),
            (2, Vectors.dense(4.0, 10.0, 8.0))
        )).toDF("id", "features")
        val scaler = new MaxAbsScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")

        val scalerModel = scaler.fit(dataFrame)
        val scaledData = scalerModel.transform(dataFrame)
        scaledData.show(false)
    }

    /**
      * 转换Vector行的数据集
      * 最大最小规范化
      * 将所有特征向量线性变换到用户指定最大-最小值。
      * 但是在计算时还是一个个特征向量分开计算的,通常将最大、最小值设置为1和0,这样就归一化到[0,1]
      * Rescaled(ei)=((ei−Emin)/(Emax−Emin)) ∗(max−min)+min
      * For the case Emax==Emin, Rescaled(ei)=0.5∗(max+min)
      */
    def minMaxScaler(spark:SparkSession): Unit = {
        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.1, -1.0)),
            (0, Vectors.dense(2.0, 1.1, 1.0)),
            (0, Vectors.dense(3.0, 10.1, 3.0))
        )).toDF("id", "features")
        val scaler = new MinMaxScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
          .setMin(0)
          .setMax(1)

        val scalerModel = scaler.fit(dataFrame)
        val scalerData = scalerModel.transform(dataFrame)
        println(s"Features scaled to range:[${scaler.getMin},${scaler.getMax}]")
        scalerData.select("features", "scaledFeatures").show(false)
    }

    /**
      * 转换Vector行的数据集
      * 标准化
      * 对于训练集中的样本,基于列统计信息将数据以方差或(且)者将数据减去其均值(结果方差等于1,数据在0附近)
      * 这是很常见的预处理步骤
      * 例如:当所有的特征值为1的方差,且/或值为0的均值时,svm的径向基函数(RBF)核或者L1和L2正则线性模型通常有更好的效果
      * 标准化可以提升模型优化阶段的收敛度,还可以避免方差很大的特征对模型训练产生过大的影响
      */
    def standardScaler(spark:SparkSession): Unit = {
        val dataFrame = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_libsvm_data.txt")
        val scaler = new StandardScaler()
            .setInputCol("features")
            .setOutputCol("scaledFeatures")
            .setWithMean(false)//默认false,在缩放之前用平均值居中数据。它将构建密集输出,因此在应用于稀疏输入时要小心
            .setWithStd(true)//默认true,将数据缩放到单位标准偏差
        val scalerModel = scaler.fit(dataFrame)
        val scalerData = scalerModel.transform(dataFrame)
        scalerData.show(false)
    }

    /**
      * 将某个特征向量（由所有样本某一个特征组成的向量）计算其p-范数，然后对该每个元素除以p-范数。
      * 将原始特征Normalizer以后可以使得机器学习算法有更好的表现。
      * 1-范数(L1)：║x║1=│x1│+│x2│+…+│xn│
      * ║x║2=（│x1│2+│x2│2+…+│xn│2） ^^ 1/2
      * ∞-范数(L∞)：║x║∞=max（│x1│，│x2│，…，│xn│）
      */
    def normalizer(spark: SparkSession): Unit = {
        val dataFrame = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 0.5, -1.0)),
            (1, Vectors.dense(2.0, 1.0, 1.0)),
            (2, Vectors.dense(4.0, 10.0, 2.0))
        )).toDF("id", "features")
        val normalizer = new Normalizer().setInputCol("features").setOutputCol("normalFeatures").setP(1.0)
        val l1NormalData = normalizer.transform(dataFrame)
        println("Normalized using L^1 norm")
        l1NormalData.show(false)
        val lInfNormal = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
        println("Normalized using L^inf norm")
        lInfNormal.show(false)
    }

    /**
      * i列j纬度的输入得到1列j ^^ i纬度
      * (x,y) (m,n) => (xm,xn,ym,yn)
      */
    def interaction(spark: SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (1, 1, 2, 3, 8, 4, 5),
            (2, 4, 3, 8, 7, 9, 8),
            (3, 6, 1, 9, 2, 3, 6),
            (4, 10, 8, 6, 9, 4, 5),
            (5, 9, 2, 7, 10, 7, 3),
            (6, 1, 1, 4, 2, 8, 4)
        )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")
        val assembler1 = new VectorAssembler().setInputCols(Array("id2","id3","id4")).setOutputCol("vec1")
        val assemblerDF1 = assembler1.transform(df)

        val assembler2 = new VectorAssembler().setInputCols(Array("id5","id6","id7")).setOutputCol("vec2")
        val assemblerDF2 = assembler2.transform(assemblerDF1).select("id1", "vec1", "vec2")

        val interaction = new Interaction().setInputCols(Array("id1", "vec1", "vec2")).setOutputCol("interaction")
        val interactionDF = interaction.transform(assemblerDF2)
        interactionDF.show(false)

    }

    /**
      * 主要作用：提高决策树或随机森林等ML方法的分类效果
      * 是对数据集特征向量中的类别（离散值）特征进行编号
      * 它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
      * 特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）
      * 某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
      */
    def vectorIndexer(spark: SparkSession): Unit = {
        val data = spark.read.format("libsvm").load("spark-examples/src/main/resources/sample_libsvm_data.txt")

        val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(10)
        val indexerModel = indexer.fit(data)

        val categoricalFeatures:Set[Int] = indexerModel.categoryMaps.keys.toSet
        println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))

        val indexerData = indexerModel.transform(data)
        indexerData.show(false)
    }

    /**
      * 独热编码 将一列标签映射为二进制向量的一列
      * 对于每一个特征，如果它有m个可能值，那么经过独热编码后，就变成了m个二元特征。
      * 并且，这些特征互斥，每次只有一个激活。因此，数据会变成稀疏的
      */
    def oneHotEncoder(spark: SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "a"),
            (4, "a"),
            (5, "c")
        )).toDF("id", "category")
        val indexer = new StringIndexer().setInputCol("category").setOutputCol("category_index").fit(df)
        val indexerDF = indexer.transform(df)

        val encoder = new OneHotEncoder().setInputCol("category_index").setOutputCol("category_vec")
        val encoderDF = encoder.transform(indexerDF)
        encoderDF.show(false)
    }

    /**
      * 与StringIndexer对称的
      * IndexToString将index映射回原先的labels
      * 通常我们使用StringIndexer产生index，然后使用模型训练数据，最后使用IndexToString找回原先的labels
      */
    def index2String(spark: SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "a"),
            (4, "a"),
            (5, "c")
        )).toDF("id", "category")
        val indexer = new StringIndexer().setInputCol("category").setOutputCol("category_indexer").fit(df)
        val indexerDF = indexer.transform(df)
        println(s"transform string column '${indexer.inputCol}' to '${indexer.outputCol}'")
        indexerDF.show()

        val inputColSchema = indexerDF.schema(indexer.getOutputCol)
        println(s"StringIndexer 将要存储输出元数据列的标签: " +
                s"${Attribute.fromStructField(inputColSchema).toString}")

        val converter = new IndexToString().setInputCol("category_indexer").setOutputCol("original_category")
        val converterDF = converter.transform(indexerDF)
        converterDF.show(false)


    }

    /**
      * 将一列labels转为[0，labels基数]的index，出现最多次labels的index为0
      */
    def stringIndexer(spark: SparkSession): Unit = {
        val df = spark.createDataFrame(Seq(
            (0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")
        )).toDF("id", "category")
        val indexer = new StringIndexer().setInputCol("category").setOutputCol("category_indexer")

        val indexerDF = indexer.fit(df).transform(df)
        indexerDF.show(false)

        val indexer2 = new StringIndexer().setInputCol("category").setOutputCol("category_indexer2").setHandleInvalid("skip")
        val indexerDF2  = indexer2.fit(df).transform(df)
        indexerDF2.show(false)
    }

    /**
      * 离散余弦变换(DCT)
      */
    def discreteCosineTransform(spark: SparkSession): Unit = {
        val data = Seq(
            Vectors.dense(0.0, 1.0, -2.0, 3.0),
            Vectors.dense(-1.0, 2.0, 4.0, -7.0),
            Vectors.dense(14.0, -2.0, -5.0, 1.0)
        )
        val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
        val dct = new DCT().setInputCol("features").setOutputCol("features_dct").setInverse(false)
        val dctDF = dct.transform(df)
        dctDF.select("features_dct").show(false)
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
