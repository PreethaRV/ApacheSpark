package com.org.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

object AirlineDelay {

  case class Airports(org_id: Int, origin: String)

  case class Routes(src: Int, dst: Int, date: String, delay: Long)



  def parseData(lines: String) = {

    val fields = lines.split(",")
    //  println(fields(11) + "%%%^&^^^^^^^^^^^^^^^^^^^^^^6")
    val year = fields(0).toInt
    val month = fields(1).toInt
    val fl_date = fields(2).toString
    val carrier = fields(3).toString
    val orig_id = fields(4).toInt
    val orig = fields(5).toString
    val orig_state = fields(6).toString
    val dest_id = fields(7).toInt
    val dest = fields(8).toString
    val dest_state = fields(9).toString
    val dep_time = fields(10).toString
    val dep_delay = fields(11).toLong

    (year, month, fl_date, carrier, orig_id, orig, orig_state, dest_id, dest, dest_state, dep_time, dep_delay)

  }

  def findMax(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b

  }
  def main(args: Array[String]) {

    //val spaConf = new SparkConf().setAppName("MarchAirlines")
    val sc = new SparkContext("local[*]", "AirlineDelay")
    //val sc = new SparkContext(spaConf)
    
    // Data used in this sample are taken from http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=OnTime
    
    val lines = sc.textFile("C:/Users/localadmin/MaprAcademy/Spark Certification/mydata/59046149_T_ONTIME/59046149_T_ONTIME.csv")
    //val lines = sc.textFile("C:/Users/localadmin/MaprAcademy/Spark Certification/mydata/59046149_T_ONTIME/904849619_T_ONTIME.csv")

    val line = lines.map(parseData)

    val airlines = line.map(x => (x._5, x._6)).distinct()
    val airlineMap = airlines.map { case (org_id, name) => (org_id -> name) }.collect.toMap

    val airports = line.map(x => (x._5, x)).groupByKey.map(x => (x._1.toLong, {
      val oriId = x._1.toInt
      val orig_name = x._2.map(y => y._6).head.toString()
      Airports(oriId, orig_name)
    }))

    airports.take(10).foreach(println)

    val routes = line.map(x => ((x._5, x._8), x)).groupByKey.map(x => (x._1, {
      val src = x._1._1.toInt
      val dest = x._1._2.toInt
      val date = x._2.map(y => y._3).head.toString
      val delay = x._2.map(y => y._12).head.toLong
      Routes(src, dest, date, delay)
    }))

    routes.take(10).foreach(println)

    val edges = routes.map(x => Edge(x._1._1.toLong, x._1._2.toLong, x._2))

    println("*******************************" + edges.first)
    val nowhere = Airports(0, "0")
    //Create the graph
    val myGraph = Graph(airports, edges, nowhere)
    
    //airport with most number of in degrees.

    val indeg = myGraph.inDegrees.reduce(findMax)

    println(airlineMap(indeg._1.toInt) + " had highest number of ->" + indeg._2 + "  incoming flight")

    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&Show how Triplet stores data &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ")
    myGraph.triplets.take(10).foreach(println)

    

    val allDelayedFlights = myGraph.triplets.sortBy(_.attr.delay, ascending = false)
    val allDisplayFlights = myGraph.triplets.map(x => (x.srcAttr.origin, x.attr.date, x.attr.delay, airlineMap(x.attr.dst.toInt)))


    //println(allDisplayFlights.filter(x=>x._2=="3/13/2016").count() + "%%%%%%%%%%%%%%%%%%%%%%%%%Count")

    //List of top 20 aiports that had highest delays for the given month
    allDisplayFlights.map(x => (x._1, x._3)).reduceByKey((a, b) => (a + b)).sortBy(f => f._2, false).collect().take(20).foreach(println)
    
    //Delays grouped for each dates for the given month.
    allDisplayFlights.map(x => (x._2, x._3)).reduceByKey((a, b) => (a + b)).sortBy(f => f._2, false).collect().foreach(println)

  }

}