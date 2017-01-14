
package com.org.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD._

object Airlines {

  
  def main(args: Array[String]) = {

   // val sparkConf = new SparkConf().setAppName("GraphxShell1")
    val sc = new SparkContext("local[*]","Airlines")

//    val sc = new SparkContext(sparkConf)
    val vertices = Array((1L, ("SFO")), (2L, ("ORD")), (3L, ("DFW")))
    val vRDD = sc.parallelize(vertices)
    val edges = Array(Edge(1L, 2L, 1800), Edge(2L, 3L, 800), Edge(3L, 1L, 1400))
    val eRDD = sc.parallelize(edges)
    // Array(Edge(1,2,1800), Edge(2,3,800))
    val nowhere = ("nowhere")
    val graph = Graph(vRDD, eRDD, nowhere)

    graph.vertices.collect.foreach(println)
    //(2,ORD)
    //(1,SFO)
    //(3,DFW)

    graph.edges.collect.foreach(println)
    //Edge(1,2,1800)
    //Edge(2,3,800)
    //Edge(3,1,1400)

    graph.triplets.collect.foreach(println)
    //((1,SFO),(2,ORD),1800)
    //((2,ORD),(3,DFW),800)
    //((3,DFW),(1,SFO),1400)

    println(graph.inDegrees)
    //VertexRDDImpl[83] at RDD at VertexRDD.scala:57

    val numairports = graph.numVertices
    //numairports: Long = 3

    val numroutes = graph.numEdges
    //numroutes: Long = 3

    //How many routes distance greater than 1000?
    graph.edges.filter { case Edge(src, dst, prop) => prop > 1000 }.count
    //res17: Long = 2

    //which routes have distance greater than 1000?
    graph.edges.filter { case Edge(src, dst, prop) => prop > 1000 }.collect.foreach(println)
    //Edge(1,2,1800)
    //Edge(3,1,1400)

    //Sort and print out the longest distance routes
    graph.triplets.sortBy(_.attr, ascending = false).map(triplet =>
      "Distance@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect.foreach(println)

    //Distance 1800 from SFO to ORD.
    //Distance 1400 from DFW to SFO.
    //Distance 800 from ORD to DFW.

    //What are the most important airports according to PageRank 
    val ranks = graph.pageRank(0.1).vertices

    ranks.take(3)
    //res0: Array[(org.apache.spark.graphx.VertexId, Double)] = Array((2,0.47799375), (1,0.47799375), (3,0.47799375))

    val impAirports = ranks.join(vRDD).sortBy(_._2._1, false).map(_._2._2)
    //ranksAndAirports: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[286] at map at <console>:44

    impAirports.collect.foreach(println)
    //ORD
    //SFO
    //DFW

  }

}