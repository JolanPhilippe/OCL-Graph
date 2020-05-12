package training.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.{RDD}

object GraphXDemo {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf()
    conf.setAppName("GraphXDemo")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"))
      )

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //SELECT src.id, dst.id, src.attr, e.attr, dst.attr
    //FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
    //ON e.srcId = src.Id AND e.dstId = dst.Id
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)


    facts.collect.foreach(println(_))

    System.out.println("-------------------------------------------")

    //SELECT src.id, dst.id, src.attr, e.attr, dst.attr
    //FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
    //ON e.srcId = src.Id AND e.dstId = dst.Id AND src.attr == "franklin" and dst.attr == "jgonzal"

    graph.triplets
      .filter(triplet => triplet.srcAttr._1 == "franklin" && triplet.dstAttr._1 == "jgonzal" )
      .map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
      .collect.foreach(println(_))


//    graph.pregel(vertex, messages => )

//    ----------------------------------------------

    val collected = graph.triplets.filter(triplet => triplet.srcAttr._1 == "franklin" && triplet.dstAttr._1 == "jgonzal" )
      .map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
      .collect

    for (t <- collected){
      println(t)
    }

  }



}
