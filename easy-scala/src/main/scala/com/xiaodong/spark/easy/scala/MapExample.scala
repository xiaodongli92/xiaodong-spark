package com.xiaodong.spark.easy.scala

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiaodong on 2017/9/12.
  */
object MapExample {
    private val map = mutable.Map[String,ArrayBuffer[String]]()

    def main(args: Array[String]): Unit = {
        val map = getMap("fruit1", "value")
        map.foreach(entry => {
            println(s"key = ${entry._1}")
            entry._2.foreach(item =>
                println(s"value = $item")
            )
        })
    }

    private def getMap(key:String, value:String):mutable.Map[String,ArrayBuffer[String]] = {
        val arr1 = ArrayBuffer[String]("a", "b", "c")
        map.put("word", arr1)
        val arr2 = ArrayBuffer[String]("apple", "banana")
        map.put("fruit", arr2)
        if (map.contains(key)) {
            map.put(key, map(key)+=value)
        } else {
            val arr = ArrayBuffer[String]()
            arr += value
            map.put(key, arr)
        }
        map
    }

}
