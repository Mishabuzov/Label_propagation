import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("project_3")
      .master("local")
      .getOrCreate()

    val sc = session.sparkContext
    sc.setLogLevel("OFF")

    val helper = new EdgesHelper(session)

    val graph = helper.loadGraph(args(0))

    println("total vertices count: ", graph.vertices.count())

    //    graph.edges.take(10).foreach(println(_)) //JUST CHECKING THAT READING IS OK!


    val labelPropagation = new LabelPropagation
    val propagatedGraph = labelPropagation.propagateLabels(graph, 10)


    // TODO: Finish evaluations: Mean, standard deviation, mode for amount of elements in a cluster.

    // 1st column VID (vertexId). 2nd column - cluster.
    val min_num_cluster = propagatedGraph
      .vertices
      .map(x => (x._2, Set(x._1)))
      .reduceByKey(_ ++ _)
      .collect()
      .minBy(_._2.size)._2.size
    println("min: ", min_num_cluster)


    val max_num_cluster = propagatedGraph
      .vertices.map(x => (x._2, Set(x._1)))
      .reduceByKey(_ ++ _)
      .collect()
      .maxBy(_._2.size)._2.size
    println("max: ", max_num_cluster)


    // Below is result saving (clustered vertices and edges).
    // TODO: UNCOMMENT IF NEED TO REFRESH  NEO4J  VIEW.
    /*
    val nodesLabels = helper.loadNodesLabels(args(1))

    propagatedGraph
      .vertices
      .join(nodesLabels)
      .map(x => String.format("%1$s,%2$s,%3$s", String.valueOf(x._1), String.valueOf(x._2._1), x._2._2))
      .saveAsTextFile("clustered_vertices")

    propagatedGraph
      .edges
      .map(x => String.format("%1$s,%2$s", String.valueOf(x.srcId), String.valueOf(x.dstId)))
      .saveAsTextFile("clustered_edges")
     */

  }


}
