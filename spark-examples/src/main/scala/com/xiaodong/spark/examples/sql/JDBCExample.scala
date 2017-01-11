package com.xiaodong.spark.examples.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by xiaodong on 2017/1/11.
  */
object JDBCExample {

    def main(args: Array[String]): Unit = {
        val warehouseLocation = "spark-warehouse"
        val spark = SparkSession
                .builder()
                .appName("Spark Hive")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate()

        hiveTables(spark)
    }

    /**
      * 支持hive
      */
    def hiveTables(spark :SparkSession): Unit = {

        import spark.implicits._
        import spark.sql
        sql("create table if not exists src (key INT, value STRING)")
        sql("load data local inpath 'spark-examples/src/main/resources/kv1.txt' in table src")

        sql("select * from src").show()

        sql("select count(*) from src").show()

    }

}
