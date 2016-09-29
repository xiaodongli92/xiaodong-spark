import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享广播变量 只读属性
  */
object BroadcastTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        conf.setAppName("Broadcast")
        conf.setMaster("local")
        val context = new SparkContext(conf)
        val array = (0 until 1000000).toArray
        for (i <- 0 until 3) {
            println("Iterator = " + i)
            val startTime = System.nanoTime()
            val barr1 = context.broadcast(array)
            println(barr1.value.length)
            val observedSize = context.parallelize(1 to 10, 2).map(_ => barr1.value.length)
            observedSize.collect().foreach(i => println(i))
            println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime() - startTime) / 1E6))

        }
    }
}
