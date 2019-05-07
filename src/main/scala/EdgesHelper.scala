import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class EdgesHelper(session: SparkSession) {

  def loadNodesLabels(path_to_labels: String): RDD[(Long, String)] = {

    session.sparkContext.textFile(path_to_labels).map(x => {
      (x.split(" ")(0).toLong, x.split(" ").drop(1).mkString(" "))
    })

  }

  def loadEdges(path_to_edges: String): RDD[(Long, Long)] = {

    session.sparkContext.textFile(path_to_edges).map(x => {
      (x.split(",")(0).toLong, x.split(",")(1).toLong)
    })

  }

  def loadGraph(path: String): Graph[VertexId, String] = {

    val allEdges = loadEdges(path)//.filter(_._1 < 100000).filter(_._2 < 100000)

    println("total edges count: ", allEdges.count())
    //    allEdges.take(10).foreach(println)

    // At the beginning of the algorithm - construct cluster labels the same, as node labels
    val vertices = allEdges.map(row => (row._1, row._1)) // (cluster number, node number (additional slot)

    val edgesTriplets = allEdges.map(row => Edge(row._1, row._2, "Link"))

    Graph(vertices, edgesTriplets)

  }

}
