package training.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkDemo")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")


//    ----------------------------------------------------------------------------------

    val input = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    input.foreach(println(_))

    System.out.println("-------------------------------------------")

    val new_input = input.map(i => i + 1).filter(i => i % 2 == 0)
    new_input.foreach(println(_))

  }

}