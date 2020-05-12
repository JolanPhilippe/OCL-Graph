package training.spark

import java.time.LocalDate

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import training.spark.SimpleOCL.Gender.Gender

//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

object SimpleOCL {

  object Gender extends Enumeration {
    type Gender = Value
    val Male: SimpleOCL.Gender.Value = Value("male")
    val Female: SimpleOCL.Gender.Value  = Value("female")
  }

  abstract class EClass extends Serializable {
    val name: String
  }

  class Person(val birthday: LocalDate, val firstName: String, val lastName: String, val gender: Gender) extends EClass{
    override val name: String = "Person"

    def age(): Int = {
      import java.time.Period
      if (this.birthday != null) Period.between(this.birthday, LocalDate.now()).getYears
      else 0
    }
  }

  def constraint_wife_husband(graph: Graph[EClass, String]): Boolean = {

    /* context Person
    * inv: self.wife->notEmpty() implies self.wife.husband = self
    *      and
    *      self.husband->notEmpty() implies self.husband.wife = self
    * */

    val husbands = graph.triplets
        .filter(triplet => triplet.srcAttr.isInstanceOf[Person]) // Set the context
        .filter(triplet => triplet.attr == "wife")
        .map(triplet => triplet.srcId).collect()
    for (id_husband <- husbands){
      if (graph.triplets.filter(triplet => triplet.attr == "husband" && triplet.dstId == id_husband).isEmpty())
        return false
    }

    val wives = graph.triplets
      .filter(triplet => triplet.srcAttr.isInstanceOf[Person]) // Set the context
      .filter(triplet => triplet.attr == "husband")
      .map(triplet => triplet.srcId).collect()
    for (id_wife <- wives){
      if (graph.triplets.filter(triplet => triplet.attr == "wife" && triplet.dstId == id_wife).isEmpty())
        return false
    }

    true
  }

  def constraint_age_lt_150(graph: Graph[EClass, String]): Boolean = {
    /* context Person
     * inv: self.age < 150
    */
    graph.vertices
      .filter(vertex => vertex._2.isInstanceOf[Person]) // Set the context
      .filter(vertex => vertex._2.asInstanceOf[Person].age() > 150)
      .count() == 0
  }

  def constraint_age_gt_0(graph: Graph[EClass, String]): Boolean = {
    /* context Person
     * inv: self.age > 0
    */
    graph.vertices
      .filter(vertex => vertex._2.isInstanceOf[Person]) // Set the context
      .filter(vertex => vertex._2.asInstanceOf[Person].age() < 0)
      .count() == 0
  }

  def confguration() : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName("GraphXDemo")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    sc
  }

  def main(args: Array[String]): Unit = {
    val sc : SparkContext = confguration()

    //    val vertices : IndexedRDD[Long, Person] = IndexedRDD(sc.parallelize(Seq(
    //      (1L, new Person(LocalDate.of(1995, 9, 27), "Jolan", "Philippe", Gender.Male)),
    //      (2L, new Person(LocalDate.of(1991, 12, 13), "Thibault", "Beziers La Fosse", Gender.Male)),
    //      (3L, new Person(LocalDate.of(1995, 1, 1), "Joachim", "Hotonnier", Gender.Male)),
    //      (4L, new Person(LocalDate.of(1998, 6, 12), "Camille", "Labonne", Gender.Female)),
    //      (5L, new Person(LocalDate.of(1989, 7, 1), "Charline", "Arceau", Gender.Female))
    //    )))

    val vertices: RDD[(VertexId, EClass)] = sc.parallelize(Array(
      (1L, new Person(LocalDate.of(1995, 9, 27), "Jolan", "Philippe", Gender.Male)),
      (2L, new Person(LocalDate.of(1991, 12, 13), "Thibault", "Beziers La Fosse", Gender.Male)),
      (3L, new Person(LocalDate.of(1995, 1, 1), "Joachim", "Hotonnier", Gender.Male)),
      (4L, new Person(LocalDate.of(1998, 6, 12), "Camille", "Labonne", Gender.Female)),
      (5L, new Person(LocalDate.of(1989, 7, 1), "Charline", "Arceau", Gender.Female))
    ))

    val edges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(1L, 4L, "husband"), Edge(4L, 1L, "wife"),
      Edge(2L, 5L, "husband"), Edge(5L, 2L, "wife")
    ))

    val graph = Graph(vertices, edges)

    assert(constraint_age_lt_150(graph))
    assert(constraint_age_gt_0(graph))
    assert(constraint_wife_husband(graph))

  }
}
