package training.spark

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ABtoCD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("GraphXDemo")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Array(
        (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"),
        (11L, "A"), (12L, "B"), (13L, "C"), (14L, "D")
      ))

    val edges: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(1L, 2L, "AtoB"),
        Edge(2L, 3L, "CtoD"),
        Edge(11L, 12L, "AtoB"),
        Edge(12L, 13L, "CtoD")
      ))

    // Define a default user in case there are relationship with missing user
    val default = "E"

    // Build the initial Graph
    val graph = Graph(vertices, edges, default)
  }


}
