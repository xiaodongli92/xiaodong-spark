package com.xiaodong.spark.examples

import org.apache.spark.sql.SparkSession


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
        interoperatingWithRDDs(spark)
    }

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
