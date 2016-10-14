import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Rosen {

  def main(args: Array[String]): Unit = {

    if(args.length <4) {
      System.err.println("Usage " + this.getClass.getName + " <stationCsvFile> <joinCsvFile> <startStationCode> <goalStationCode> [superStep=20]")
      System.exit(1)
    }
    val stationCsvFile = args(0)
    val joinCsvFile = args(1)
    val startStaCode = args(2).toLong
    val goalStaCode = args(3).toLong
    val superStep = if(args.length >= 5) args(4).toInt else 20

    val sc = new SparkContext(new SparkConf())

    val stations: RDD[(VertexId, (String, Long, String))] = sc.textFile(stationCsvFile).map(line => {
      val lines = line.split(",")
      (lines(0).toLong, (lines(2), 0L, ""))
    })

    val joinStations: RDD[Edge[(Long, Int)]] = sc.textFile(joinCsvFile).flatMap(line => {
      val lines = line.split(",")
      Seq(Edge(lines(1).toLong, lines(2).toLong, (1L, lines(0).toInt)), Edge(lines(2).toLong, lines(1).toLong, (1L, lines(0).toInt)))
    })

    val sameStations: RDD[Edge[(Long, Int)]] = sc.textFile(stationCsvFile).flatMap(line => {
      val lines = line.split(",")
      if(lines(0) == lines(1)) Seq.empty
      else Seq(Edge(lines(0).toInt, lines(1).toInt, (0L, -1)), Edge(lines(1).toInt, lines(0).toInt, (0L, -1)))
    })

    var graph: Graph[(String, Long, String), (Long, Int)] = Graph(stations, joinStations.union(sameStations), ("", 0L, ""))

    graph = graph.subgraph(edge => (edge.attr._2 >= 28001 && edge.attr._2 <= 28010) || edge.attr._2 == -1, (a, b) => true)


    val sssp = graph.pregel[(Long, String)]((10000000000L, ""), superStep, EdgeDirection.Either)(
      (stationCode, state, message) => {
        if(stationCode == startStaCode) (state._1, 0, state._1)
        else (state._1, message._1, message._2)
      },
      triplet => {
        if(triplet.dstAttr._3 == "" || triplet.srcAttr._2 + triplet.attr._1 <= triplet.dstAttr._2) {
          if(triplet.attr._1 == 0) Iterator((triplet.dstId, ((triplet.srcAttr._2 + triplet.attr._1), triplet.srcAttr._3)))
          else Iterator((triplet.dstId, ((triplet.srcAttr._2 + triplet.attr._1), triplet.srcAttr._3 + "->" + triplet.dstAttr._1)))
        } else {
          Iterator.empty
        }
      },
      (message1, message2) => if(message1._1 < message2._1) message1 else message2
    )

    println(sssp.vertices.filter(i => i._1 == goalStaCode).collect.mkString("\n"))

    sc.stop()
  }
}

