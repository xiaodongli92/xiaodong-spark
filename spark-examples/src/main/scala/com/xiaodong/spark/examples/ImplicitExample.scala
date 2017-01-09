package com.xiaodong.spark.examples

/**
  * Created by lixiaodong on 17/1/7.
  */
class ImplicitExample(a : A) {
    def trans: Unit = {
        println("trans...")
    }
}

class A {

}

object ImplicitExample {

    def main(args: Array[String]): Unit = {
        //隐式转换
        val a = new A()
        a.trans
        //隐式参数
        implicit val name = "name"
        implicitMethod
        implicitMethod("test")
        //隐式类
        println(10.add(2))
    }



    //隐式类
    implicit class JiSuan(x: Int) {
        def add(a: Int): Int = a + x
    }

    def implicitMethod(implicit name: String): Unit = {
        println(name)
    }

    //隐式转换
    implicit def a2Im(a:A): ImplicitExample = {
        return new ImplicitExample(a)
    }

}
