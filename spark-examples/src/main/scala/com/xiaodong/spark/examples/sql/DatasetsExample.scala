package com.xiaodong.spark.examples.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by lixiaodong on 17/1/7.
  */
object DatasetsExample {

    case class Person(name: String, age: Integer)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder()
          .appName("DatasetsExample1")
          .master("local")
          .getOrCreate()
//        creatingDataFramesFromJson(spark)
//        interoperatingWithRDDs(spark)
//        programmaticallySpecifyingScheme(spark)
//        genericLoad(spark)
//        runSqlOnFiles(spark)
//        manuallySpecifyingOptions(spark)
//        schemeMerging(spark)
        jsonDataSets(spark)

    }

    /**
      * json数据集
      */
    def jsonDataSets(spark: SparkSession): Unit = {
        val path = "spark-examples/src/main/resources/people.json"
        val peopleDF = spark.read.json(path)
        peopleDF.printSchema()
        peopleDF.show()

        peopleDF.createOrReplaceTempView("people")

        val teenagerNamesDF = spark.sql("select name from people where age between 13 and 19")
        teenagerNamesDF.show()

        val otherPeopleRDD = spark.sparkContext.makeRDD("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
        val otherPeopleDF = spark.read.json(otherPeopleRDD)
        otherPeopleDF.show()
    }

    /**
      * 模式合并
      * 由于模式合并是一个相对昂贵的操作，大多数情况不是必须的，从1.5.0开始默认关闭
      * 启用方式：1数据源选项设置mergeSchema到true 2设置全局SQL选项spark.sql.parquet.mergeSchema为true
      */
    def schemeMerging(spark: SparkSession): Unit = {
        import spark.implicits._
        val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i*i)).toDF("value", "square")
        squaresDF.write.parquet("data/test_table/key=1")

        val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i*i*i)).toDF("value", "cube")
        cubesDF.write.parquet("data/test_table/key=2")

        val mergeDF = spark.read.option("mergeScheme", "true").parquet("data/test_table")
        mergeDF.printSchema()

        mergeDF.show
    }

    /**
      * parquet直接对文件运行sql
      */
    def runSqlOnFiles(spark: SparkSession): Unit = {
        val sqlDF = spark.sql("select * from parquet.`spark-examples/src/main/resources/users.parquet`")
        sqlDF.show()
    }

    /**
      * 手动指定选项
      */
    def manuallySpecifyingOptions(spark: SparkSession): Unit = {
        val peopleDF = spark.read.format("json").load("spark-examples/src/main/resources/people.json")
        peopleDF.select("name","age").write.format("parquet").save("nameAndAges.parquet")
    }

    /**
      * parquet 列式存储格式
      * 已经被spark设为默认的文件存储格式
      * 通用加载保存
      */
    def genericLoad(spark: SparkSession): Unit = {
        spark.sparkContext.textFile("spark-examples/src/main/resources/users.parquet").foreach(println)
        val userDF = spark.read.load("spark-examples/src/main/resources/users.parquet")
        userDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
        userDF.show()
    }

    /**
      * 以编程方式指定模式
      */
    def programmaticallySpecifyingScheme(spark: SparkSession): Unit = {
        import spark.implicits._
        val peopleRDD = spark.sparkContext.textFile("spark-examples/src/main/resources/people.txt")
        val schemeStr = "name age"
        val fields = schemeStr.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val scheme = StructType(fields)
        val rowRDD = peopleRDD.map(_.split(",")).map(values => Row(values(0).trim, values(1).trim))
        val peopleDF = spark.createDataFrame(rowRDD, scheme)
        peopleDF.createOrReplaceTempView("people")
        val results = spark.sql("select name from people")
        results.map(values => "name = " + values(0)).show
    }

    /**
      * 利用反射来推断模式
      */
    def interoperatingWithRDDs(spark: SparkSession): Unit = {
        import spark.implicits._
        val peopleDF = spark.sparkContext.textFile("spark-examples/src/main/resources/people.txt")
          .map(_.split(","))
          .map(members => Person(members(0), members(1).trim.toInt))
          .toDF()
        peopleDF.createOrReplaceTempView("people")
        val targetDF = spark.sql("select name, age from people where age between 0 and 100")
        targetDF.show()
        targetDF.map(one => "name = " + one(0)).show()
        targetDF.map(one => "age => " + one.getAs[Integer]("age")).show()

        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
        targetDF.map(one => one.getValuesMap[Any](Seq("name", "age"))).collect().foreach(println)
    }

    /**
      * 从JSON文件加载为DataFrame
      */
    def creatingDataFramesFromJson(spark: SparkSession): Unit = {
        import spark.implicits._
        //从JSON文件创建DataFrame
        val df = spark.read.json("spark-examples/src/main/resources/people.json")
        df.show()
        //非类型数据集操作(也称DataFrame操作)
        df.printSchema()
        df.select("name").show()
        df.select("*").show()
        df.groupBy($"age" + 1).count().show()

        df.createOrReplaceTempView("people")
        val sqlDF = spark.sql("SELECT * FROM people")
        sqlDF.show()

        df.createGlobalTempView("people")
        spark.sql("SELECT * FROM global_tmp.people").show()
        spark.newSession().sql("SELECT * FROM global_tmp.people").show()

        spark.sql("show databases").show()
        spark.sql("use default")
        spark.sql("show tables").show()

    }

}
