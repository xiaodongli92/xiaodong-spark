package com.xiaodong.spark.easy.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by lixiaodong on 16/10/6.
  */
object PeopleJson {

    case class Person(name:String, age:Long)

    def main(args: Array[String]): Unit = {
        val ss = SparkSession.builder().appName("PeopleJson").master("local").getOrCreate()
//        showPeople(ss)
        dataSetCreation(ss)
    }

    def dataSetCreation(ss: SparkSession):Unit = {
        import ss.implicits._
        val personDS = Seq(Person("name", 18)).toDS()
        personDS.show()

        val person = ss.read.json("/Users/lixiaodong/Documents/input/people.json").as[Person]
        person.show()

        val peoples = ss.read.json("/Users/lixiaodong/Documents/input/people.json")
        peoples.sort("name").show()
    }

    def showPeople(ss: SparkSession):Unit = {
        val peoples = ss.read.json("/Users/lixiaodong/Documents/input/people.json")
        peoples.show()
        peoples.printSchema()
        peoples.createOrReplaceTempView("peoples")
        ss.sql("select * from peoples where age>10").show()
        peoples.select("name").show()
        ss.sql("select name from peoples").show()
    }
}
